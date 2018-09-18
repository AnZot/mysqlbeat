// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Query struct {
	Type string `config:"type"`
	SQL  string `config:"sql"`
}

type Config struct {
	Period            time.Duration `config:"period"`
	Hostname          string        `config:"hostname"`
	Port              string        `config:"port"`
	Username          string        `config:"username"`
	Password          string        `config:"password"`
	EncryptedPassword string        `config:"encryptedpassword"`
	Queries           []Query       `config:"queries"`
	DeltaWildcard     string        `config:"deltawildcard"`
	DeltaKeyWildcard  string        `config:"deltakeywildcard"`
}

var DefaultConfig = Config{
	Period:            1 * time.Second,
	Hostname:          "",
	Port:              "",
	Username:          "",
	Password:          "",
	EncryptedPassword: "",
	Queries:           []Query{},
	DeltaWildcard:     "",
	DeltaKeyWildcard:  "",
}
