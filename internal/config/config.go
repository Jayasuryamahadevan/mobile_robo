package config

import (
 "fmt"
 "os"
 "strconv"
 "strings"
 "time"
)
type Config struct {
 DBURL,RedisAddr,NATSURL,Port,JWTSecret,Issuer,Audience string
 OllamaURL,EmbeddingModel,QdrantURL,Neo4jURL,Neo4jUser,Neo4jPassword string
 VectorEnabled,GraphEnabled bool
 WorkerPoll time.Duration
}
func get(name,fallback string) string { if v:=strings.TrimSpace(os.Getenv(name));v!="" {return v};return fallback }
func Load() (Config,error) {
 c:=Config{
  DBURL:get("DATABASE_URL",""),
  RedisAddr:get("REDIS_ADDR","localhost:6379"),
  NATSURL:get("NATS_URL","nats://localhost:4222"),
  Port:get("PORT","8080"),
  JWTSecret:get("JWT_SECRET",""),
  Issuer:get("JWT_ISSUER","phone-robot"),
  Audience:get("JWT_AUDIENCE","memory-api"),
  OllamaURL:get("OLLAMA_URL","http://localhost:11434"),
  EmbeddingModel:get("EMBEDDING_MODEL","nomic-embed-text"),
  QdrantURL:get("QDRANT_URL","http://localhost:6333"),
  Neo4jURL:get("NEO4J_URL","http://localhost:7474"),
  Neo4jUser:get("NEO4J_USERNAME","neo4j"),
  Neo4jPassword:get("NEO4J_PASSWORD",""),
  VectorEnabled:get("MEMORY_VECTOR_ENABLED","false")=="true",
  GraphEnabled:get("MEMORY_GRAPH_ENABLED","false")=="true",
 }
 ms,e:=strconv.Atoi(get("WORKER_POLL_MS","500"))
 if e!=nil||ms<50 {return c,fmt.Errorf("invalid WORKER_POLL_MS")}
 c.WorkerPoll=time.Duration(ms)*time.Millisecond
 if c.DBURL=="" {return c,fmt.Errorf("DATABASE_URL is required")}
 if len(c.JWTSecret)<32 {return c,fmt.Errorf("JWT_SECRET must have at least 32 characters")}
 return c,nil
}
