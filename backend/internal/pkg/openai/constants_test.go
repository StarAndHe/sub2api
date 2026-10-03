package openai

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultModelsIncludeBareGPT56Alias(t *testing.T) {
	require.Contains(t, DefaultModelIDs(), "gpt-5.6")
}

func TestDefaultModelsPreferConcreteGPT56SolForAccountTests(t *testing.T) {
	require.NotEmpty(t, DefaultModels)
	require.Equal(t, "gpt-5.6-sol", DefaultModels[0].ID)
}

func TestDefaultModelsIncludeGPT6Family(t *testing.T) {
	ids := DefaultModelIDs()
	require.Contains(t, ids, "gpt-6.1-sol")
	require.Contains(t, ids, "gpt-6-sol")
	require.Contains(t, ids, "gpt-6-luna")
	require.Contains(t, ids, "gpt-6-astra")
}
