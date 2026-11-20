package canonical

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"
)

// ReconcileSummary reports one replica-reconciliation pass.
type ReconcileSummary struct {
	RF              int
	Shards          int
	UnderBefore     int
	CopiesCreated   int
	UnderAfter      int
	UnreachableNode []string
	DryRun          bool
}

type replicaNode struct {
	groupID   int32
	name      string
	port      int32
	reachable bool
}

type replicaPlacement struct {
	shardID int64
	groupID int32
	state   int
}

type copyOp struct {
	shardID int64
	source  replicaNode
	target  replicaNode
}

// planReplicas selects the copies needed to bring every shard up to rf
// reachable active placements. It never copies from an unreachable node and
// never targets a node that already has any placement for the shard. Targets
// are balanced by their current active-placement count.
func planReplicas(rf int, shards []int64, placements []replicaPlacement, nodes []replicaNode, load map[int32]int) []copyOp {
	if rf < 1 {
		rf = 1
	}
	byShard := map[int64][]replicaPlacement{}
	for _, p := range placements {
		byShard[p.shardID] = append(byShard[p.shardID], p)
	}
	byGroup := map[int32]replicaNode{}
	for _, n := range nodes {
		byGroup[n.groupID] = n
	}
	ordered := append([]int64(nil), shards...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	ops := []copyOp{}
	for _, shardID := range ordered {
		owned := map[int32]bool{}
		active := []replicaNode{}
		for _, p := range byShard[shardID] {
			owned[p.groupID] = true
			if p.state == 1 {
				if n, ok := byGroup[p.groupID]; ok && n.reachable {
					active = append(active, n)
				}
			}
		}
		if len(active) == 0 || len(active) >= rf {
			continue
		}
		source := active[0]
		for len(active) < rf {
			best := replicaNode{}
			found := false
			for _, n := range nodes {
				if !n.reachable || owned[n.groupID] {
					continue
				}
				if !found || load[n.groupID] < load[best.groupID] ||
					(load[n.groupID] == load[best.groupID] && n.groupID < best.groupID) {
					best = n
					found = true
				}
			}
			if !found {
				break
			}
			ops = append(ops, copyOp{shardID: shardID, source: source, target: best})
			owned[best.groupID] = true
			active = append(active, best)
			// The copy cascades to the colocated outbox shard, so the target
			// gains two placements, not one.
			load[best.groupID] += 2
		}
	}
	return ops
}

// ReconcileReplicas restores the colocation replication factor after a node
// loss. citus_rebalance_start only balances placements that still exist; it
// does not recreate copies removed when a node was disabled, so this pass
// uses citus_copy_shard_placement directly. Copying an events shard also
// copies its colocated outbox shard.
func (s *Store) ReconcileReplicas(ctx context.Context, dryRun bool) (ReconcileSummary, error) {
	sum := ReconcileSummary{DryRun: dryRun}
	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return sum, err
	}
	defer conn.Close()
	var locked bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(70407002)`).Scan(&locked); err != nil {
		return sum, err
	}
	if !locked {
		log.Print("replica reconciler: another run holds the advisory lock; skipping")
		return sum, nil
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock(70407002)`)
	}()

	// Never copy placements while Citus is moving shards: the two operations
	// can race and leave duplicate placements behind.
	var rebalanceRunning bool
	if err = conn.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM citus_rebalance_status() WHERE state = 'running')`).Scan(&rebalanceRunning); err != nil {
		log.Printf("replica reconciler: cannot read rebalance status: %v", err)
	} else if rebalanceRunning {
		log.Print("replica reconciler: a Citus rebalance is running; skipping")
		return sum, nil
	}
	if _, err = conn.ExecContext(ctx, `SET citus.node_connection_timeout = '2000ms'`); err != nil {
		log.Printf("replica reconciler: cannot set node connection timeout: %v", err)
	}
	if err = conn.QueryRowContext(ctx, `SELECT c.replicationfactor
		FROM pg_dist_partition p JOIN pg_dist_colocation c ON c.colocationid=p.colocationid
		WHERE p.logicalrelid='canonical.events'::regclass`).Scan(&sum.RF); err != nil {
		return sum, fmt.Errorf("read colocation replication factor: %w", err)
	}
	if sum.RF < 1 {
		sum.RF = 1
	}

	nodeRows, err := conn.QueryContext(ctx, `SELECT groupid, nodename, nodeport
		FROM pg_dist_node WHERE isactive AND noderole='primary' AND shouldhaveshards
		ORDER BY nodeid`)
	if err != nil {
		return sum, err
	}
	nodes := []replicaNode{}
	for nodeRows.Next() {
		var n replicaNode
		if err = nodeRows.Scan(&n.groupID, &n.name, &n.port); err != nil {
			nodeRows.Close()
			return sum, err
		}
		nodes = append(nodes, n)
	}
	err = nodeRows.Err()
	nodeRows.Close()
	if err != nil {
		return sum, err
	}
	for i := range nodes {
		var ok bool
		if err = conn.QueryRowContext(ctx, `SELECT success FROM master_run_on_worker(
			ARRAY[$1]::text[], ARRAY[$2]::int4[], ARRAY['SELECT 1']::text[], false)`,
			nodes[i].name, nodes[i].port).Scan(&ok); err != nil {
			log.Printf("replica reconciler: probe %s failed: %v", nodes[i].name, err)
			sum.UnreachableNode = append(sum.UnreachableNode, nodes[i].name)
			continue
		}
		nodes[i].reachable = ok
		if !ok {
			sum.UnreachableNode = append(sum.UnreachableNode, nodes[i].name)
		}
	}
	// A node that is still active in Citus metadata but unreachable may be a
	// transient outage that will return with its copies intact. Copying now
	// would leave a third copy when the node returns. Wait until the operator
	// declares the loss (citus_disable_node/remove_node) or the node recovers.
	if len(sum.UnreachableNode) > 0 {
		log.Printf("replica reconciler: active node(s) %v unreachable; deferring until they recover or are declared lost", sum.UnreachableNode)
		return sum, nil
	}

	shardRows, err := conn.QueryContext(ctx, `SELECT shardid FROM pg_dist_shard
		WHERE logicalrelid='canonical.events'::regclass ORDER BY shardid`)
	if err != nil {
		return sum, err
	}
	shards := []int64{}
	for shardRows.Next() {
		var id int64
		if err = shardRows.Scan(&id); err != nil {
			shardRows.Close()
			return sum, err
		}
		shards = append(shards, id)
	}
	err = shardRows.Err()
	shardRows.Close()
	if err != nil {
		return sum, err
	}
	sum.Shards = len(shards)

	placementRows, err := conn.QueryContext(ctx, `SELECT p.shardid, p.groupid, p.shardstate
		FROM pg_dist_placement p JOIN pg_dist_shard s ON s.shardid=p.shardid
		WHERE s.logicalrelid='canonical.events'::regclass`)
	if err != nil {
		return sum, err
	}
	placements := []replicaPlacement{}
	for placementRows.Next() {
		var p replicaPlacement
		if err = placementRows.Scan(&p.shardID, &p.groupID, &p.state); err != nil {
			placementRows.Close()
			return sum, err
		}
		placements = append(placements, p)
	}
	err = placementRows.Err()
	placementRows.Close()
	if err != nil {
		return sum, err
	}

	load := map[int32]int{}
	loadRows, err := conn.QueryContext(ctx, `SELECT n.groupid, count(*)
		FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid)
		WHERE p.shardstate=1 AND n.isactive GROUP BY n.groupid`)
	if err != nil {
		return sum, err
	}
	for loadRows.Next() {
		var groupID int32
		var count int
		if err = loadRows.Scan(&groupID, &count); err != nil {
			loadRows.Close()
			return sum, err
		}
		load[groupID] = count
	}
	err = loadRows.Err()
	loadRows.Close()
	if err != nil {
		return sum, err
	}

	reachable := map[int32]bool{}
	for _, n := range nodes {
		reachable[n.groupID] = n.reachable
	}
	activePerShard := map[int64]int{}
	for _, p := range placements {
		if p.state == 1 && reachable[p.groupID] {
			activePerShard[p.shardID]++
		}
	}
	for _, id := range shards {
		if activePerShard[id] < sum.RF {
			sum.UnderBefore++
		}
	}

	ops := planReplicas(sum.RF, shards, placements, nodes, load)
	var copyErr error
	for _, op := range ops {
		if dryRun {
			log.Printf("replica reconciler: dry-run copy shard=%d %s:%d -> %s:%d",
				op.shardID, op.source.name, op.source.port, op.target.name, op.target.port)
			sum.CopiesCreated++
			continue
		}
		if _, err = conn.ExecContext(ctx, `SELECT citus_copy_shard_placement(
			$1::bigint,$2::text,$3::int,$4::text,$5::int,'auto')`,
			op.shardID, op.source.name, op.source.port, op.target.name, op.target.port); err != nil {
			log.Printf("replica reconciler: copy shard=%d %s -> %s failed: %v",
				op.shardID, op.source.name, op.target.name, err)
			if copyErr == nil {
				copyErr = err
			}
			continue
		}
		log.Printf("replica reconciler: copied shard=%d %s:%d -> %s:%d",
			op.shardID, op.source.name, op.source.port, op.target.name, op.target.port)
		sum.CopiesCreated++
	}

	afterRows, err := conn.QueryContext(ctx, `SELECT p.shardid, p.groupid, p.shardstate
		FROM pg_dist_placement p JOIN pg_dist_shard s ON s.shardid=p.shardid
		WHERE s.logicalrelid='canonical.events'::regclass`)
	if err != nil {
		return sum, err
	}
	after := map[int64]int{}
	for afterRows.Next() {
		var p replicaPlacement
		if err = afterRows.Scan(&p.shardID, &p.groupID, &p.state); err != nil {
			afterRows.Close()
			return sum, err
		}
		if p.state == 1 && reachable[p.groupID] {
			after[p.shardID]++
		}
	}
	err = afterRows.Err()
	afterRows.Close()
	if err != nil {
		return sum, err
	}
	for _, id := range shards {
		if after[id] < sum.RF {
			sum.UnderAfter++
		}
	}
	if sum.UnderAfter > 0 && copyErr == nil {
		copyErr = fmt.Errorf("%d shards remain below replication factor %d", sum.UnderAfter, sum.RF)
	}
	return sum, copyErr
}
