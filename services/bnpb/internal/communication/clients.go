package communication

import "time"

type Clients struct {
	BMKG  *BMKGClient
	PVMBG *PVMBGClient
}

func New(bmkgAddress, bmkgKey, pvmbgURL, pvmbgToken string, timeout time.Duration) (*Clients, error) {
	bmkg, err := newBMKGClient(bmkgAddress, bmkgKey, timeout)
	if err != nil {
		return nil, err
	}
	return &Clients{
		BMKG:  bmkg,
		PVMBG: newPVMBGClient(pvmbgURL, pvmbgToken, timeout),
	}, nil
}

func (c *Clients) Close() error {
	c.PVMBG.http.CloseIdleConnections()
	return c.BMKG.Close()
}
