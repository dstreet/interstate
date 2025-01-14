package interstate_test

import (
	"testing"

	"github.com/dstreet/interstate"
	"github.com/dstreet/interstate/mocks"
	"github.com/stretchr/testify/assert"
)

func TestVersionUpdate(t *testing.T) {
	versionWriter := mocks.NewVersionWriter(t)
	v := uint64(5)
	version := interstate.NewVersion(v, []byte("initial-data"), versionWriter)

	versionWriter.EXPECT().
		Write(v, []byte("new-data")).
		Return(nil)

	err := version.Update([]byte("new-data"))
	assert.NoError(t, err)

	versionWriter.AssertExpectations(t)
}
