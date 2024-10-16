package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type Storage interface {
	CreateAccount(*Account) error
	DeleteAccount(int) error
	UpdateAccount(*Account) error
	GetAccounts() ([]*Account, error)
	GetAccountByID(int) (*Account, error)
}

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore() (*SQLiteStore, error) {
	connStr := "./sqlite-db.db"
	db, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &SQLiteStore{
		db: db,
	}, nil
}

func (s *SQLiteStore) Init() error {
	return s.CreateTable()
}

func (s *SQLiteStore) CreateTable() error {
	query := `create table if not exists accounts (
		id integer primary key autoincrement,
		first_name varchar(50),
		last_name varchar(50),
		number integer,
		balance integer,
		created_at timestamp
	)`

	_, err := s.db.Exec(query)

	return err
}

func (s *SQLiteStore) CreateAccount(acc *Account) error {
	query := `insert into 
		accounts (first_name, last_name, number, balance, created_at)
		values (?, ?, ?, ?, ?)`
	_, err := s.db.Exec(
		query,
		acc.FirstName,
		acc.LastName,
		acc.Number,
		acc.Balance,
		acc.CreatedAt,
	)

	if err != nil {
		return err
	}

	return nil
}

func (s *SQLiteStore) DeleteAccount(id int) error {

	query := `delete from accounts where id = ?`

	_, err := s.db.Exec(query, id)

	return err
}

func (s *SQLiteStore) UpdateAccount(*Account) error {
	return nil
}

func (s *SQLiteStore) GetAccountByID(id int) (*Account, error) {
	query := `select 
		id, 
		first_name, 
		last_name, 
		number, 
		balance, 
		created_at 
		from accounts
		where id = ?`

	rows, err := s.db.Query(query, id)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		return scanIntoAccount(rows)
	}

	return nil, fmt.Errorf("account %d not found", id)

}

func (s *SQLiteStore) GetAccounts() ([]*Account, error) {
	query := `select 
		id, 
		first_name, 
		last_name, 
		number, 
		balance, 
		created_at 
		from accounts`

	rows, err := s.db.Query(query)

	if err != nil {
		return nil, err
	}

	accounts := []*Account{}

	defer rows.Close()

	for rows.Next() {
		account, err := scanIntoAccount(rows)
		if err != nil {
			return nil, err
		}

		accounts = append(accounts, account)
	}
	return accounts, nil
}

func scanIntoAccount(rows *sql.Rows) (*Account, error) {
	account := &Account{}
	err := rows.Scan(&account.ID,
		&account.FirstName,
		&account.LastName,
		&account.Number,
		&account.Balance,
		&account.CreatedAt)
	if err != nil {
		return nil, err
	}
	return account, nil
}
