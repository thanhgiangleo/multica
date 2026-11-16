package telegram

import "testing"

func TestIsOTPCommand(t *testing.T) {
	cases := []struct {
		name string
		text string
		want bool
	}{
		{"bare", "/get_multica_otp", true},
		{"with bot suffix", "/get_multica_otp@SomeBot", true},
		{"case insensitive", "/GET_MULTICA_OTP", true},
		{"leading whitespace", "  /get_multica_otp", true},
		{"hyphenated form is not the command", "/get-multica-otp", false},
		{"prefix of another token", "/get_multica_otpx", false},
		{"not first token", "please /get_multica_otp", false},
		{"empty", "", false},
		{"other command", "/issue hi", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isOTPCommand(tc.text, "SomeBot"); got != tc.want {
				t.Fatalf("isOTPCommand(%q) = %v, want %v", tc.text, got, tc.want)
			}
		})
	}
}

func TestOTPAdminIDs(t *testing.T) {
	t.Setenv(otpAdminEnv, " 902276034 , 42 ,bad, ")
	ids := otpAdminIDs()
	if !ids[902276034] || !ids[42] {
		t.Fatalf("expected both valid ids, got %v", ids)
	}
	if len(ids) != 2 {
		t.Fatalf("malformed entries must be skipped, got %v", ids)
	}

	t.Setenv(otpAdminEnv, "")
	if got := otpAdminIDs(); got != nil {
		t.Fatalf("empty env must disable the command, got %v", got)
	}
}
