package domain

import "context"

type ContextBrokerClient interface {
	Post() error
}

type contextBrokerClient struct{}

func (c *contextBrokerClient) Post() error {
	return nil
}

func NewContextBrokerClient(ctx context.Context) ContextBrokerClient {
	return &contextBrokerClient{}
}
