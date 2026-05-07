package test

type a interface {
	hello() int
}
type b struct {
}

func (b *b) hello() int {
	//TODO implement me
	panic("implement me")
}
func newA2() a {
	return newB2()
}

func newB2() *b {
	return &b{}
}
