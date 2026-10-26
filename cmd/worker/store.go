package main

import (
	"bufio"
	"fmt"
	"os"
	"syscall"
)

// eventStore records processed event_ids in a plain text file, one per line.
// The file is shared by every worker process, so a duplicate is caught even
// when it lands on a different worker than the original. flock serializes
// access between processes.
type eventStore struct {
	path string
}

func (s *eventStore) Seen(eventID string) (bool, error) {
	f, err := os.OpenFile(s.path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		return false, err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if sc.Text() == eventID {
			return true, nil
		}
	}
	return false, sc.Err()
}

func (s *eventStore) Mark(eventID string) error {
	f, err := os.OpenFile(s.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	_, err = fmt.Fprintln(f, eventID)
	return err
}
