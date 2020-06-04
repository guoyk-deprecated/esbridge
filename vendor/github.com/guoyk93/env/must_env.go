package env

func MustStringVar(out *string, key string, defaultValue string) {
	if err := StringVar(out, key, defaultValue); err != nil {
		panic("invalid environment variable:" + key + ":" + err.Error())
	}
}

func MustUint64Var(out *uint64, key string, defaultValue uint64) {
	if err := Uint64Var(out, key, defaultValue); err != nil {
		panic("invalid environment variable:" + key + ":" + err.Error())
	}
}

func MustInt64Var(out *int64, key string, defaultValue int64) {
	if err := Int64Var(out, key, defaultValue); err != nil {
		panic("invalid environment variable:" + key + ":" + err.Error())
	}
}

func MustFloat64Var(out *float64, key string, defaultValue float64) {
	if err := Float64Var(out, key, defaultValue); err != nil {
		panic("invalid environment variable:" + key + ":" + err.Error())
	}
}

func MustBoolVar(out *bool, key string, defaultValue bool) {
	if err := BoolVar(out, key, defaultValue); err != nil {
		panic("invalid environment variable:" + key + ":" + err.Error())
	}
}
