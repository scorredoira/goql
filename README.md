# Goql

## SQL parser and query builder

```go
q, err := ParseQuery("select * from foo where id = ?")
if err != nil {
    log.Fatal(err)
}
```