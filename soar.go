package mysqlx

import (
	"os"
	"os/exec"
)

var soarEnable = func() bool {
	_, err := exec.LookPath("soar")
	return err == nil && os.Getenv("MYSQL_SOAR_ENABLE") != ""
}()

func soar(query string) (string, error) {
	cmd := exec.Command("soar", "-query", query)
	out, err := cmd.Output()
	return string(out), err
}
