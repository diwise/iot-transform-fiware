package projection

import "net/url"

// Config är anslutningskonfiguration för projektionslagringen (samma form
// som regellagret: enda vägen är cmd-flaggor via NewConfig; A äger
// flaggorna och wiringen).
type Config struct {
	host     string
	user     string
	password string
	port     string
	dbname   string
	sslmode  string
}

func NewConfig(host, user, password, port, dbname, sslmode string) Config {
	return Config{
		host:     host,
		user:     user,
		password: password,
		port:     port,
		dbname:   dbname,
		sslmode:  sslmode,
	}
}

func (c Config) ConnStr() string {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.user, c.password),
		Host:   c.host + ":" + c.port,
		Path:   "/" + c.dbname,
	}
	q := u.Query()
	if c.sslmode != "" {
		q.Set("sslmode", c.sslmode)
		u.RawQuery = q.Encode()
	}
	return u.String()
}
