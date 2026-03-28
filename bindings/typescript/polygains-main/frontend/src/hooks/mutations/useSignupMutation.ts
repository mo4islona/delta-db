"use client";
import useSWRMutation from "swr/mutation";
import { postSignup, type SignupResponse } from "@/api/terminalApi";

async function signupFetcher(
	_url: string,
	{ arg }: { arg: string },
): Promise<SignupResponse> {
	return postSignup(arg);
}

export function useSignupMutation() {
	const mutation = useSWRMutation<SignupResponse, Error, string, string>(
		"signup",
		signupFetcher,
	);

	return {
		signup: mutation.trigger,
		isLoading: mutation.isMutating,
		data: mutation.data,
		error: mutation.error,
		reset: mutation.reset,
	};
}
