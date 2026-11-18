package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/umohsamuel/transactional-outbox/internal/services"
)

type HttpServer struct {
	router   *gin.Engine
	services *services.Services
}

func NewHttpServer(svc *services.Services) *HttpServer {
	r := gin.Default()
	r.ContextWithFallback = true
	s := &HttpServer{router: r, services: svc}

	s.setupRoutes()
	return s
}

func (s *HttpServer) setupRoutes() {

	// v1 := s.router.Group("/api/v1")

	// authMW := middlewares.NewAuth(s.services.Auth)
	// protected := v1.Group("", authMW.Handle)

	// mfaHandler := handlers.NewMFAHandler(s.services)
	// mfaHandler.RegisterRoutes(protected)

}

func (s *HttpServer) Handler() http.Handler {
	return s.router
}
