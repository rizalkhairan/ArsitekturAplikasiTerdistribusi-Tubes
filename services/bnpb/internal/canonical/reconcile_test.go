package canonical

import "testing"

func replicaNodes() []replicaNode {
	return []replicaNode{
		{groupID: 1, name: "node-1", port: 5432, reachable: true},
		{groupID: 2, name: "node-2", port: 5432, reachable: true},
		{groupID: 3, name: "node-3", port: 5432, reachable: true},
		{groupID: 4, name: "node-4", port: 5432, reachable: false},
	}
}

func TestPlanReplicasRestoresMissingCopies(t *testing.T) {
	nodes := replicaNodes()
	placements := []replicaPlacement{
		{shardID: 1, groupID: 1, state: 1},
		{shardID: 2, groupID: 1, state: 1},
		{shardID: 2, groupID: 2, state: 1},
		{shardID: 3, groupID: 4, state: 1},
	}
	ops := planReplicas(2, []int64{1, 2, 3}, placements, nodes, map[int32]int{})
	if len(ops) != 1 {
		t.Fatalf("ops=%d, want 1: %+v", len(ops), ops)
	}
	if ops[0].shardID != 1 || ops[0].target.groupID != 2 {
		t.Fatalf("unexpected op: %+v", ops[0])
	}
}

func TestPlanReplicasSkipsUnreachableSource(t *testing.T) {
	placements := []replicaPlacement{{shardID: 1, groupID: 4, state: 1}}
	ops := planReplicas(2, []int64{1}, placements, replicaNodes(), map[int32]int{})
	if len(ops) != 0 {
		t.Fatalf("ops=%+v, want none", ops)
	}
}

func TestPlanReplicasBalancesTargets(t *testing.T) {
	placements := []replicaPlacement{{shardID: 1, groupID: 1, state: 1}}
	load := map[int32]int{2: 10, 3: 2}
	ops := planReplicas(2, []int64{1}, placements, replicaNodes(), load)
	if len(ops) != 1 || ops[0].target.groupID != 3 {
		t.Fatalf("unexpected ops: %+v", ops)
	}
}

func TestPlanReplicasNeverTargetsExistingPlacement(t *testing.T) {
	placements := []replicaPlacement{
		{shardID: 1, groupID: 1, state: 1},
		{shardID: 1, groupID: 2, state: 2},
	}
	ops := planReplicas(2, []int64{1}, placements, replicaNodes(), map[int32]int{2: 0, 3: 9})
	if len(ops) != 1 || ops[0].target.groupID != 3 {
		t.Fatalf("unexpected ops: %+v", ops)
	}
}
