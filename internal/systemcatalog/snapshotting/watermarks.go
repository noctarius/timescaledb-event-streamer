package snapshotting

type Watermarks struct {
	watermarks map[string]*watermark
}

func (w *Watermarks) MarshalBinary() (data []byte, err error) {
	//TODO implement me
	panic("implement me")
}

func (w *Watermarks) UnmarshalBinary(data []byte) error {
	//TODO implement me
	panic("implement me")
}

type watermark struct {
	high map[string]any
	low  map[string]any
}
