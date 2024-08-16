# PostgresSQL Database Package

## Examples

### Migrations

```go
migrator := NewMigrator(pathToMigrations, os.Getenv("DSN"))
err := migrator.Run()
```

### CRUD

Check it [here](/repository/dao/dao_test.go).

### Tests

Create .env file and up test docker container:
    
    make env

Run tests: 

    make test
