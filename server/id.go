package server

import "fmt"

// IDSequence allocates IDs from 1 through maxID, reusing released IDs on wrap.
type IDSequence[ID ~uint16 | ~uint64] struct {
	current ID
	maxID   ID
	usedIDs map[ID]struct{}
}

func NewIDSequence[ID ~uint16 | ~uint64](maxID ID) *IDSequence[ID] {
	return &IDSequence[ID]{maxID: maxID, usedIDs: make(map[ID]struct{})}
}

// Next returns and reserves the next available ID.
func (s *IDSequence[ID]) Next() (ID, error) {
	if s.maxID == 0 {
		return 0, fmt.Errorf("limite de identificadores não pode ser zero")
	}
	if uint64(len(s.usedIDs)) >= uint64(s.maxID) {
		return 0, fmt.Errorf("limite de %d identificadores atingido", s.maxID)
	}

	id := s.current
	// At most len(usedIDs) candidates can be occupied.
	for range len(s.usedIDs) + 1 {
		if id >= s.maxID {
			id = 1
		} else {
			id++
		}
		if _, exists := s.usedIDs[id]; !exists {
			if s.usedIDs == nil {
				s.usedIDs = make(map[ID]struct{})
			}
			s.current = id
			s.usedIDs[id] = struct{}{}
			return id, nil
		}
	}
	return 0, fmt.Errorf("nenhum identificador disponível")
}

// Release makes an ID available for future allocations.
func (s *IDSequence[ID]) Release(id ID) {
	delete(s.usedIDs, id)
}
