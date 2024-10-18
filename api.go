package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	jwt "github.com/golang-jwt/jwt/v5"
)

func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(v)
}

type apiFunc func(http.ResponseWriter, *http.Request) error

type ApiError struct {
	Error string `json:"error"`
}

func makeHTTPHandleFunc(f apiFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := f(w, r); err != nil {
			WriteJSON(w, http.StatusInternalServerError, ApiError{Error: err.Error()})
		}
	}
}

type APIServer struct {
	listenAddr string
	store      Storage
}

func NewAPIServer(listenAddr string, store Storage) *APIServer {
	return &APIServer{
		listenAddr: listenAddr,
		store:      store,
	}
}

func (s *APIServer) Run() {
	router := http.NewServeMux()

	router.HandleFunc("POST /account", makeHTTPHandleFunc(s.handleCreateAccount))
	router.HandleFunc("GET /account", makeHTTPHandleFunc(s.handleGetAccount))
	router.HandleFunc("GET /account/{id}", withJWTAuth(makeHTTPHandleFunc(s.handleGetAccountByID)))
	router.HandleFunc("DELETE /account/{id}", withJWTAuth(makeHTTPHandleFunc(s.handleDeleteAccount)))

	router.HandleFunc("POST /transfer", makeHTTPHandleFunc(s.handleTransfer))

	log.Println("JSON API Server running on port: ", s.listenAddr)

	http.ListenAndServe(s.listenAddr, router)
}

func (s *APIServer) handleAccount(w http.ResponseWriter, r *http.Request) error {
	if r.Method == "GET" {
		return s.handleGetAccount(w, r)
	}

	if r.Method == "POST" {
		return s.handleCreateAccount(w, r)
	}

	if r.Method == "DELETE" {
		return s.handleDeleteAccount(w, r)
	}
	return fmt.Errorf("Method not allowed %s", r.Method)
}

func (s *APIServer) handleGetAccountByID(w http.ResponseWriter, r *http.Request) error {
	id, err := getID(r)

	if err != nil {
		return err
	}

	account, err := s.store.GetAccountByID(id)

	if err != nil {
		return err
	}

	return WriteJSON(w, http.StatusOK, account)
}

func (s *APIServer) handleCreateAccount(w http.ResponseWriter, r *http.Request) error {
	createAccountRequest := CreateAccountRequest{}
	if err := json.NewDecoder(r.Body).Decode(&createAccountRequest); err != nil {
		return err
	}

	account := NewAccount(createAccountRequest.FirstName, createAccountRequest.LastName)
	id, err := s.store.CreateAccount(account)

	if err != nil {
		return err
	}

	account.ID = id
	token, err := createJWT(account)

	if err != nil {
		return err
	}

	return WriteJSON(w, http.StatusOK, map[string]string{"token": token})
}

func (s *APIServer) handleDeleteAccount(w http.ResponseWriter, r *http.Request) error {

	id, err := getID(r)

	if err != nil {
		return err
	}

	err = s.store.DeleteAccount(id)

	if err != nil {
		return err
	}

	return WriteJSON(w, http.StatusOK, map[string]int{"deleted": id})
}

func (s *APIServer) handleTransfer(w http.ResponseWriter, r *http.Request) error {
	transferRequest := &TranseferRequest{}

	if err := json.NewDecoder(r.Body).Decode(transferRequest); err != nil {
		return err
	}
	defer r.Body.Close()

	return WriteJSON(w, http.StatusOK, transferRequest)
}

func (s *APIServer) handleGetAccount(w http.ResponseWriter, r *http.Request) error {
	accounts, err := s.store.GetAccounts()

	if err != nil {
		return err
	}

	return WriteJSON(w, http.StatusOK, accounts)
}

func getID(r *http.Request) (int, error) {
	idStr := r.PathValue("id")

	id, err := strconv.Atoi(idStr)

	if err != nil {
		return id, fmt.Errorf("invalid id given: %s", idStr)
	}

	return id, nil
}

func withJWTAuth(handlerFunc http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("With JWT Auth middleware")
		tokenString := r.Header.Get("x-jwt-token")
		if len(tokenString) == 0 {
			permissionDenied(w)
			return
		}

		token, err := validateJWT(tokenString)

		if err != nil || !token.Valid {
			permissionDenied(w)
			return
		}

		claims := token.Claims.(*MyCustomClaims)

		id, err := getID(r)

		if err != nil {
			permissionDenied(w)
			return
		}

		if claims.AccountID != id {
			permissionDenied(w)
			return
		}

		handlerFunc(w, r)
	}
}

type MyCustomClaims struct {
	AccountNumber int64 `json:"accountNumber"`
	AccountID     int   `json:"id"`
	jwt.RegisteredClaims
}

const secret = "shh"

func validateJWT(tokenString string) (*jwt.Token, error) {
	token, err := jwt.ParseWithClaims(tokenString, &MyCustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(secret), nil
	})
	if err != nil {
		log.Fatal(err)
	} else if claims, ok := token.Claims.(*MyCustomClaims); ok {
		fmt.Println(claims.AccountNumber, claims.RegisteredClaims.Issuer)
	} else {
		log.Fatal("unknown claims type, cannot proceed")
	}
	return token, err
}

func createJWT(account *Account) (string, error) {

	mySigningKey := []byte(secret)

	// Create the Claims
	claims := &MyCustomClaims{
		AccountNumber: account.Number,
		AccountID:     account.ID,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(mySigningKey)
}

func permissionDenied(w http.ResponseWriter) {
	WriteJSON(w, http.StatusForbidden, ApiError{Error: "permission denied"})
}
