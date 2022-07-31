package model

import "sync"

type Cashe struct {
	sync.RWMutex
	Memory map[string]Order
}

func New() *Cashe {
	return &Cashe{Memory: make(map[string]Order)}
}
