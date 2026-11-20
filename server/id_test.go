package server

import "testing"

func TestIDSequence(t *testing.T) {
	s := NewIDSequence(uint16(3))
	for want := uint16(1); want <= 3; want++ {
		got, err := s.Next()
		if err != nil || got != want {
			t.Fatalf("Next() = %d, %v; want %d", got, err, want)
		}
	}
	if _, err := s.Next(); err == nil {
		t.Fatal("expected an error when all IDs are reserved")
	}
	if s.current != 3 || len(s.usedIDs) != 3 {
		t.Fatal("failed allocation changed sequence state")
	}
	s.Release(2)
	s.Release(2)
	s.Release(0)
	s.Release(99)
	got, err := s.Next()
	if err != nil || got != 2 {
		t.Fatalf("Next() after release = %d, %v; want 2", got, err)
	}
	if _, err := s.Next(); err == nil {
		t.Fatal("reused ID was not reserved")
	}
}

func TestIDSequenceLimits(t *testing.T) {
	t.Run("zero limit", func(t *testing.T) {
		s := NewIDSequence(uint16(0))
		if _, err := s.Next(); err == nil {
			t.Fatal("expected an error for a zero limit")
		}
		if s.current != 0 || len(s.usedIDs) != 0 {
			t.Fatal("failed allocation changed sequence state")
		}
	})
	t.Run("single ID", func(t *testing.T) {
		s := NewIDSequence(uint16(1))
		for range 2 {
			got, err := s.Next()
			if err != nil || got != 1 {
				t.Fatalf("Next() = %d, %v; want 1", got, err)
			}
			s.Release(got)
		}
	})
	t.Run("uint16 boundary", func(t *testing.T) {
		s := NewIDSequence(^uint16(0))
		s.current = ^uint16(0) - 1
		for _, want := range []uint16{65535, 1, 2} {
			got, err := s.Next()
			if err != nil || got != want {
				t.Fatalf("Next() = %d, %v; want %d", got, err, want)
			}
		}
	})
	t.Run("uint64 boundary", func(t *testing.T) {
		s := NewIDSequence(^uint64(0))
		s.current = ^uint64(0) - 1
		for _, want := range []uint64{^uint64(0), 1, 2} {
			got, err := s.Next()
			if err != nil || got != want {
				t.Fatalf("Next() = %d, %v; want %d", got, err, want)
			}
		}
	})
}
