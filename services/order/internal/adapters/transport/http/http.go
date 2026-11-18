package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/adapters/transport/http/handlers"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/services"
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
	v1 := s.router.Group("/api/v1")

	handlers.NewOrderHandler(s.services).RegisterRoutes(v1)

}

func (s *HttpServer) Handler() http.Handler {
	return s.router
}
