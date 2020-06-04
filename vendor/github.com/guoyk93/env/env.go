package env

import (
	"os"
	"strconv"
	"strings"
)

func StringVar(out *string, key string, defaultValue string) error {
	val := strings.TrimSpace(os.Getenv(key))
	if len(val) == 0 {
		*out = defaultValue
	} else {
		*out = val
	}
	return nil
}

func Uint64Var(out *uint64, key string, defaultValue uint64) error {
	sv := strings.TrimSpace(os.Getenv(key))
	if len(sv) == 0 {
		*out = defaultValue
		return nil
	}
	if v, err := strconv.ParseUint(sv, 10, 64); err != nil {
		return err
	} else {
		*out = v
		return nil
	}
}

func Int64Var(out *int64, key string, defaultValue int64) error {
	sv := strings.TrimSpace(os.Getenv(key))
	if len(sv) == 0 {
		*out = defaultValue
		return nil
	}
	if v, err := strconv.ParseInt(sv, 10, 64); err != nil {
		return err
	} else {
		*out = v
		return nil
	}
}

func Float64Var(out *float64, key string, defaultValue float64) error {
	sv := strings.TrimSpace(os.Getenv(key))
	if len(sv) == 0 {
		*out = defaultValue
		return nil
	}
	if v, err := strconv.ParseFloat(sv, 64); err != nil {
		return err
	} else {
		*out = v
		return nil
	}
}

func BoolVar(out *bool, key string, defaultValue bool) error {
	sv := strings.TrimSpace(os.Getenv(key))
	if len(sv) == 0 {
		*out = defaultValue
		return nil
	}
	if v, err := strconv.ParseBool(sv); err != nil {
		return err
	} else {
		*out = v
		return nil
	}
}
