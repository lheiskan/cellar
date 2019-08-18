package cellar

import (
	"io/ioutil"
	"os"
	"testing"
)

func getFolder() string {
	dir, err := ioutil.TempDir("testdata", "cellar")
	if err != nil {
		panic(err)
	}
	return dir
	//return NewTempFolder("cellar")
	//return "testdata/cellar"
}

func TestMain(m *testing.M) {
	// setup
	retCode := m.Run()
	RemoveTempFolders()
	os.Exit(retCode)
}

func makeSlice(l int) []byte {
	return make([]byte, l)
}
