package api

type Coordinator interface {
	Submit(cmd []byte) (int64, int64, bool)
	Shutdown() error
}
