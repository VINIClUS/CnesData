package apiclient

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestIdentidadeAWSMantemClaimsNulos(t *testing.T) {
	for _, profile := range []struct {
		name        string
		email       any
		displayName any
	}{
		{name: "nulos", email: nil, displayName: nil},
		{name: "preenchidos", email: "gestor@example.test", displayName: "Gestor municipal"},
	} {
		t.Run(profile.name, func(t *testing.T) {
			expected := map[string]any{
				"subject":      "cognito-sem-uuid",
				"issuer":       "https://idp.example.test",
				"email":        profile.email,
				"display_name": profile.displayName,
			}
			fields := roundtripAWSIdentity(t, expected)
			if !reflect.DeepEqual(fields, expected) {
				t.Fatalf("identidade_alterada=%v esperado=%v", fields, expected)
			}
		})
	}
}

func roundtripAWSIdentity(t *testing.T, expected map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	var response AuthMeApiV1DashboardAuthMeGet200JSONResponseBody
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	identity, err := response.AsAwsMeResponse()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}
