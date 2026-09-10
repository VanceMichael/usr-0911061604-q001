package main

import "github.com/gin-gonic/gin"

func main() {
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.Run(":8080")
}
