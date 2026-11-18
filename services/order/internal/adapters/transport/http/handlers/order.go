package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/domain/order"
	"github.com/umohsamuel/transactional-outbox/services/order/internal/services"
)

type OrderHandler struct {
	svc *services.OrderService
}

func NewOrderHandler(s *services.Services) *OrderHandler {
	return &OrderHandler{svc: s.Order}
}

func (h *OrderHandler) RegisterRoutes(r gin.IRouter) {
	g := r.Group("/orders")
	g.POST("", h.Create)
	g.GET("/:id", h.Get)
}

type createOrderRequest struct {
	CustomerID uuid.UUID `json:"customer_id" binding:"required"`
	Amount     int64     `json:"amount" binding:"required,gt=0"`
}

func (h *OrderHandler) Create(c *gin.Context) {
	var req createOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	o, err := h.svc.CreateOrder(c, req.CustomerID, req.Amount)
	switch {
	case errors.Is(err, order.ErrInvalidAmount), errors.Is(err, order.ErrInvalidCustID):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create order"})
		return
	}

	c.JSON(http.StatusCreated, o)
}

func (h *OrderHandler) Get(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid order id"})
		return
	}

	o, err := h.svc.GetOrder(c, id)
	switch {
	case errors.Is(err, order.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not get order"})
		return
	}

	c.JSON(http.StatusOK, o)
}
