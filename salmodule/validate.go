package salmodule

import "fmt"

type validateCmd struct{}

func (cmd *validateCmd) Run() error {
	return fmt.Errorf("salmodule validate is not yet implemented")
}
