package version

import (
	"fmt"
	"github.com/go-errors/errors"
	"regexp"
	"strconv"
)

var (
	postgresqlVersionRegex  = regexp.MustCompile(`^((1[0-9])\.([0-9]+))?`)
	timescaledbVersionRegex = regexp.MustCompile(`([0-9]+)\.([0-9]+)(\.([0-9]+))?`)
)

type PostgresVersion uint

func (pv PostgresVersion) Major() uint {
	return uint(pv) / 10000
}

func (pv PostgresVersion) Minor() uint {
	return uint(pv) % (100)
}

func (pv PostgresVersion) String() string {
	return fmt.Sprintf("%d.%d", pv.Major(), pv.Minor())
}

func (pv PostgresVersion) Compare(other PostgresVersion) int {
	if pv < other {
		return -1
	}
	if other > pv {
		return 1
	}
	return 0
}

func ParsePostgresVersion(version string) (PostgresVersion, error) {
	matches := postgresqlVersionRegex.FindStringSubmatch(version)
	if len(matches) < 3 {
		return 0, errors.Errorf("failed to extract postgresql version")
	}

	v, err := strconv.ParseInt(matches[2], 10, 32)
	if err != nil {
		return 0, err
	}
	major := uint(v)

	v, err = strconv.ParseInt(matches[3], 10, 32)
	if err != nil {
		return 0, err
	}
	minor := uint(v)

	return PostgresVersion((major * 10000) + minor), nil
}

type TimescaleVersion uint

func (tv TimescaleVersion) Major() uint {
	return uint(tv) / 10000
}

func (tv TimescaleVersion) Minor() uint {
	return (uint(tv) / 100) % 100
}

func (tv TimescaleVersion) Release() uint {
	return uint(tv) % (100)
}

func (tv TimescaleVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", tv.Major(), tv.Minor(), tv.Release())
}

func (tv TimescaleVersion) Compare(other TimescaleVersion) int {
	if tv < other {
		return -1
	}
	if other > tv {
		return 1
	}
	return 0
}

func ParseTimescaleVersion(version string) (TimescaleVersion, error) {
	matches := timescaledbVersionRegex.FindStringSubmatch(version)
	if len(matches) < 3 {
		return 0, errors.Errorf("failed to extract timescale version")
	}

	v, err := strconv.ParseInt(matches[1], 10, 32)
	if err != nil {
		return 0, err
	}
	major := uint(v)

	v, err = strconv.ParseInt(matches[2], 10, 32)
	if err != nil {
		return 0, err
	}
	minor := uint(v)

	release := uint(0)
	if len(matches) == 5 {
		v, err = strconv.ParseInt(matches[4], 10, 32)
		if err != nil {
			return 0, err
		}
		release = uint(v)
	}

	return TimescaleVersion((major * 10000) + (minor * 100) + release), nil
}
