/**
 * Auth Reducer - Pure logic for authentication state changes
 */

export interface User {
	id: string;
	address: string;
	email?: string;
	name?: string;
	avatar?: string;
}

export interface AuthState {
	user: User | null;
	isAuthenticated: boolean;
	isLoading: boolean;
	error: string | null;
}

export type AuthAction =
	| { type: "AUTH_START" }
	| { type: "AUTH_SUCCESS"; payload: User }
	| { type: "AUTH_FAILURE"; payload: string }
	| { type: "AUTH_LOGOUT" }
	| { type: "AUTH_CLEAR_ERROR" }
	| { type: "AUTH_UPDATE_USER"; payload: Partial<User> };

export const initialAuthState: AuthState = {
	user: null,
	isAuthenticated: false,
	isLoading: false,
	error: null,
};

export function authReducer(state: AuthState, action: AuthAction): AuthState {
	switch (action.type) {
		case "AUTH_START":
			return {
				...state,
				isLoading: true,
				error: null,
			};

		case "AUTH_SUCCESS":
			return {
				...state,
				user: action.payload,
				isAuthenticated: true,
				isLoading: false,
				error: null,
			};

		case "AUTH_FAILURE":
			return {
				...state,
				user: null,
				isAuthenticated: false,
				isLoading: false,
				error: action.payload,
			};

		case "AUTH_LOGOUT":
			return {
				...initialAuthState,
			};

		case "AUTH_CLEAR_ERROR":
			return {
				...state,
				error: null,
			};

		case "AUTH_UPDATE_USER":
			return {
				...state,
				user: state.user
					? { ...state.user, ...action.payload }
					: null,
			};

		default:
			return state;
	}
}
