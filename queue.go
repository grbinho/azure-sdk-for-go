/*
	This should be queue package as a sub backage of storage package.
*/
package storage

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

type QueueStorageClient struct {
	client StorageClient
}

type Queue struct {
	Name   string
	client *QueueStorageClient
}

type GetQueueMessagesResponse struct {
	XMLName           xml.Name       `xml:"QueueMessagesList"`
	QueueMessagesList []QueueMessage `xml:"QueueMessage"`
}

type QueueMessage struct {
	XMLName         xml.Name `xml:"QueueMessage"`
	MessageId       string   `xml:"MessageId"`
	InsertionTime   string   `xml:"InsertionTime"`
	ExpirationTime  string   `xml:"EpirationTime"`
	PopReceipt      string   `xml:"PopReceipt"`
	TimeNextVisible string   `xml:"TimeNextVisible"`
	DequeueCount    int      `xml:"DequeueCount"`
	MessageText     string   `xml:"MessageText"`
}

//Constants that define HTTP verbs
const (
	HTTPGet     = "GET"
	HTTPPut     = "PUT"
	HTTPPost    = "POST"
	HTTPDelete  = "DELETE"
	HTTPPatch   = "PATCH"
	HTTPOptions = "OPTIONS"
	HTTPHead    = "HEAD"
	HTTPTrace   = "TRACE"
	HTTPConnect = "CONNECT"
)

//Constants that define custom ms headers
const (
	X_MS_POPRECEIPT = "x-ms-popreceipt"
)

//GetQueue returns a reference to a queue.
func (client *QueueStorageClient) GetQueue(name string) (*Queue, error) {
	var queue *Queue
	var err error

	//TODO: throw error if queue name is not lowercase

	//TODO: Somehow check if queue exists?

	//Check if queue exists by getting queue metadata
	queue, err = client.getQueueMetadata(name)
	fmt.Println("Get Queue: Received queue")

	if err != nil {
		return nil, err
	}
	//If queue does not exist, try to create one
	if queue == nil {
		fmt.Println("Get Queue: Queue was nil")
		queue, err = client.createQueue(name)
		if err != nil {
			return nil, err
		}
	}

	fmt.Println("Creating queue")
	fmt.Printf("%+v", queue)

	return queue, err

	//return &Queue{Name: name, client: client}, nil
}

//CreateQueue operation creates a queue under the given account.
func (client *QueueStorageClient) CreteQueue(name string) (*Queue, error) {
	return client.CreteQueue(name)
}

//DeleteQueue operation permanently deletes queue.
func (q *QueueStorageClient) DeleteQueue(name string) error {
	return q.deleteQueue(name)
}

//GetQueueMetadata operation retrieves user-defined metadata and queue properties on the specified queue.
//Metadata is associated with the queue as name-values pairs.
func (q *QueueStorageClient) GetQueueMetadata(name string) {

}

//SetQueueMetadata operation sets user-defined metadata on the specified queue.
//Metadata is associated with the queue as name-value pairs.
func (q *QueueStorageClient) SetQueueMetadata(name string) {

}

//GetQueueACL operation returns details about any stored access policies specified on the queue that may be used with Shared Access Signatures.
func (q *QueueStorageClient) GetQueueACL(name string) {
}

//SetQueueACL operation sets stored access policies for the queue that may be used with Shared Access Signatures.
func (q *QueueStorageClient) SetQueueACL(name string) {

}

//ListQueues operation lists all of the queues in a given storage account.
func (q *QueueStorageClient) ListQueues() {

}

//SetQueueServiceProperties operation sets properties for a storage account’s Queue service endpoint, including properties for Storage Analytics and CORS (Cross-Origin Resource Sharing) rules.
func (q *QueueStorageClient) SetQueueServiceProperties() {

}

//GetQueueServiceProperties operation gets the properties of a storage account’s Queue service, including properties for Storage Analytics and CORS (Cross-Origin Resource Sharing) rules.
func (q *QueueStorageClient) GetQueueServiceProperties() {

}

//PreflightQueue Request operation queries the Cross-Origin Resource Sharing (CORS) rules for the Queue service prior to sending the actual request
func (q *QueueStorageClient) PreflightQueueRequest() {

}

//GetQueueServiceStats operation retrieves statistics related to replication for the Queue service. It is only available on the secondary location endpoint when read-access geo-redundant replication is enabled for the storage account.
func (q *QueueStorageClient) GetQueueServiceStats() {

}

//PutMessage operation adds a new message to the back of the message queue.
func (q *Queue) PutMessage(message string) error {
	return q.client.putMessageSimple(q.Name, message)
}

//GetMessage operation first message from the queue
func (q *Queue) GetMessage() (QueueMessage, error) {
	messages, err := q.client.getMessages(q.Name, 1, 10, 30)

	if len(messages) > 0 {
		return messages[0], err
	}

	return QueueMessage{}, err
}

//GetMessages operation retrieves one or more messages from the front of the queue.
func (q *Queue) GetMessages(numberOfMessages int) ([]QueueMessage, error) {
	messages, err := q.client.getMessages(q.Name, numberOfMessages, 10, 30)
	return getMessages(messages, err)
}

//PeekMessages operation retrieves one or more messages from the front of the queue, but does not alter the visibility of the message.
func (q *Queue) PeekMessages(numberOfMessages int) ([]QueueMessage, error) {
	messages, err := q.client.peekMessages(q.Name, numberOfMessages)
	return getMessages(messages, err)
}

//DeleteMessage operation deletes the specified message.
func (q *Queue) DeleteMessage(message QueueMessage) error {
	return q.client.deleteMessage(q.Name, message.MessageId, message.PopReceipt, 30)
}

//ClearMessages operation deletes all messages from the specified queue.
func (q *Queue) ClearMessages() error {
	return q.client.clearMessages(q.Name)
}

//UpdateMessage operation updates the visibility timeout of a message. You can also use this operation to update the contents of a message.
func (q *Queue) UpdateMessage(message QueueMessage) (QueueMessage, error) {
	return q.client.updateMessage(q.Name, message, 10, 30)
}

//PUT https://myaccount.queue.core.windows.net/myqueue
//Documentation: https://msdn.microsoft.com/en-us/library/azure/dd179342.aspx
func (q *QueueStorageClient) createQueue(name string) (*Queue, error) {
	//compose uri
	uri := q.client.getEndpoint(queueServiceName, pathForQueue(name), nil)

	headers := q.client.getStandardHeaders()
	headers["Content-Length"] = "0"
	response, err := q.client.exec(HTTPPut, uri, headers, nil)

	if err != nil {
		return nil, err
	}

	//queue was created (201-Created)
	if response.statusCode == http.StatusCreated {
		queue := &Queue{Name: name, client: q}
		return queue, nil
	}

	//queue already exists (204-NoContent)
	if response.statusCode == http.StatusNoContent {
		//queue already exists
		return &Queue{Name: name, client: q}, nil
	}

	return nil, nil
}

//DELETE https://myaccount.queue.core.windows.net/myqueue
//Documentation: https://msdn.microsoft.com/en-us/library/azure/dd179436.aspx
func (q *QueueStorageClient) deleteQueue(name string) error {
	//compose uri
	uri := q.client.getEndpoint(queueServiceName, pathForQueue(name), nil)
	//compose and execute request
	headers := q.client.getStandardHeaders()
	response, err := q.client.exec(HTTPDelete, uri, headers, nil)

	if err != nil {
		return err
	}

	//Success returs 204 (NoContent)
	if response.statusCode != http.StatusNoContent {
		//Some error occured. Compose error and return it
		err = fmt.Errorf("Queue: Error occured deleting queue. Status (%s). Body:", response.statusCode, readResponseBodyAsString(response.body))
	}

	return nil
}

//GET https://myaccount.queue.core.windows.net/myqueue?comp=metadata
//Documentation: https://msdn.microsoft.com/en-us/library/azure/dd179384.aspx
func (q *QueueStorageClient) getQueueMetadata(name string) (*Queue, error) {
	uri := q.client.getEndpoint(queueServiceName, pathForQueue(name), url.Values{"comp": {"metadata"}})

	fmt.Printf("%+v\n", uri)

	headers := q.client.getStandardHeaders()

	response, err := q.client.exec(HTTPGet, uri, headers, nil)

	fmt.Println("Got the getQueue response")

	if err != nil {
		fmt.Println("Got an error")
		fmt.Printf("%+v", err)
		return nil, err
	}

	//Inspect storage response and return Queue struct if possible
	if response.statusCode == 200 {
		fmt.Println("Received 200")
		return &Queue{Name: name, client: q}, nil
	}

	fmt.Println("Queue does not exist")

	return nil, nil
}

func pathForQueue(queueName string) string {
	return fmt.Sprintf("/%s", queueName)
}

func pathForMessages(queueName string) string {
	return fmt.Sprintf("/%s/messages", queueName)
}

func pathForMessage(queueName string, messageId string) string {
	return fmt.Sprintf("/%s/messages/%s", queueName, messageId)
}

func (q *QueueStorageClient) putMessageSimple(queueName string, message string) error {
	return q.putMessage(queueName, message, 0, 3600, 30)
}

//POST	https://myaccount.queue.core.windows.net/myqueue/messages?visibilitytimeout=<int-seconds>&messagettl=<int-seconds>
//Documentation: https://msdn.microsoft.com/en-us/library/azure/dd179346.aspx
func (q *QueueStorageClient) putMessage(queueName string, messageContent string, visibilityTimeout int, messageTtl int, timeout int) error {
	//Compose uri
	uri := q.client.getEndpoint(queueServiceName, pathForMessages(queueName),
		url.Values{"visibilitytimeout": {strconv.Itoa(visibilityTimeout)},
			"messagettl": {strconv.Itoa(messageTtl)},
			"timeout":    {strconv.Itoa(timeout)}})

	//compose and execute request
	messageBody := fmt.Sprintf("<QueueMessage>\n<MessageText>%s</MessageText>\n</QueueMessage>", messageContent)
	headers := q.client.getStandardHeaders()
	headers["Content-Length"] = strconv.Itoa(len(messageBody))
	response, err := q.client.exec(HTTPPost, uri, headers, bytes.NewBuffer([]byte(messageBody)))

	if err != nil {
		return err
	}

	//Success returns 201 (Created)
	if response.statusCode != http.StatusCreated {
		//Some error occured. Compose error and return it
		err = fmt.Errorf("Queue: Error occured creating message. Status (%s). Body:", response.statusCode, readResponseBodyAsString(response.body))
	}

	return nil
}

//PUT https://myaccount.queue.core.windows.net/myqueue/messages/messageid?popreceipt=<string-value>&visibilitytimeout=<int-seconds>
//Documentation: https://msdn.microsoft.com/en-us/library/azure/hh452234.aspx
func (q *QueueStorageClient) updateMessage(queueName string, message QueueMessage, visibilityTimeout int, timeout int) (QueueMessage, error) {
	//compose uri
	uri := q.client.getEndpoint(queueServiceName, pathForMessage(queueName, message.MessageId),
		url.Values{"visibilitytimeout": {strconv.Itoa(visibilityTimeout)},
			"popreceipt": {message.PopReceipt},
			"timeout":    {strconv.Itoa(timeout)}})

	//compose and execute request
	messageBody := fmt.Sprintf("<QueueMessage>\n<MessageText>%s</MessageText>\n</QueueMessage>", message.MessageText)
	headers := q.client.getStandardHeaders()
	headers["Content-Length"] = strconv.Itoa(len(messageBody))
	response, err := q.client.exec(HTTPPut, uri, headers, bytes.NewBuffer([]byte(messageBody)))

	if err != nil {
		return QueueMessage{}, err
	}

	//Success returns 204 (NoContent)
	if response.statusCode != http.StatusNoContent {
		//Some error occured. Compose error and return it
		err = fmt.Errorf("Queue: Error occured updating message. Status (%s). Body:", response.statusCode, readResponseBodyAsString(response.body))
	}

	//We get new popReceipt in headers (x-ms-popreceipt), message needs to be updated
	message.PopReceipt = response.headers[X_MS_POPRECEIPT][0]

	return message, nil
}

//GET https://myaccount.queue.core.windows.net/myqueue/messages?peekonly=true&numofmessages
//Documentation: https://msdn.microsoft.com/en-us/library/azure/dd179472.aspx
func (q *QueueStorageClient) peekMessages(queueName string, numberOfMessages int) ([]QueueMessage, error) {
	//compose uri
	uri := q.client.getEndpoint(queueServiceName, pathForMessages(queueName),
		url.Values{"peekonly": {"true"}, "numofmessages": {strconv.Itoa(numberOfMessages)}})

	//compose and execute request
	headers := q.client.getStandardHeaders()
	response, err := q.client.exec(HTTPGet, uri, headers, nil)

	if err != nil {
		return nil, err
	}

	//Success returns 200
	if response.statusCode != http.StatusOK {
		//Some error occured. Compose error and return it
		err = fmt.Errorf("Queue: Error occured getting messages. Status (%s). Body:", response.statusCode, readResponseBodyAsString(response.body))
	}

	var out GetQueueMessagesResponse
	err = xmlUnmarshal(response.body, &out)
	return out.QueueMessagesList, err
}

//GET https://myaccount.queue.core.windows.net/myqueue/messages
//Documentation: https://msdn.microsoft.com/en-us/library/azure/dd179474.aspx
func (q *QueueStorageClient) getMessages(queueName string, numberOfMessages int, visibilityTimeout int, timeout int) ([]QueueMessage, error) {

	//Parameter validation
	//Optional: numberOfMessages >0 <=32, default 1
	switch {
	case numberOfMessages <= 0:
		numberOfMessages = 1
	case numberOfMessages > 32:
		numberOfMessages = 32
	}

	//Optional: visibilityTimeout. A specified value must be larger than or equal to 1 second, and cannot be larger than 7 days, default 30 (seconds)
	switch {
	case visibilityTimeout < 1:
		visibilityTimeout = 1
	case visibilityTimeout > 604800: //seven days
		visibilityTimeout = 604800
	}

	//Optional: timeout. Maximum is 30 sec. Service itself reduces any timeout over 30 sec
	if timeout == 0 {
		timeout = 30
	}

	//compose uri
	uri := q.client.getEndpoint(queueServiceName, pathForMessages(queueName),
		url.Values{"numofmessages": {strconv.Itoa(numberOfMessages)},
			"visibilitytimeout": {strconv.Itoa(visibilityTimeout)},
			"timeout":           {strconv.Itoa(timeout)}})

	//compose and execute request
	headers := q.client.getStandardHeaders()
	response, err := q.client.exec(HTTPGet, uri, headers, nil)

	if err != nil {
		return nil, err
	}

	//Success returs 200
	if response.statusCode != http.StatusOK {
		//Some error occured. Compose error and return it
		err = fmt.Errorf("Queue: Error occured getting messages. Status (%s). Body:", response.statusCode, readResponseBodyAsString(response.body))
	}

	var out GetQueueMessagesResponse
	err = xmlUnmarshal(response.body, &out)
	return out.QueueMessagesList, err
}

//DELETE https://myaccount.queue.core.windows.net/myqueue/messages/messageid?popreceipt=string-value
//Documentation: https://msdn.microsoft.com/en-us/library/azure/dd179347.aspx
func (q *QueueStorageClient) deleteMessage(queueName string, messageId string, popReceipt string, timeout int) error {
	//compose uri
	uri := q.client.getEndpoint(queueServiceName, pathForMessage(queueName, messageId), url.Values{"popreceipt": {popReceipt}})
	//compose and execute request
	headers := q.client.getStandardHeaders()
	response, err := q.client.exec(HTTPDelete, uri, headers, nil)

	if err != nil {
		return err
	}

	//Success returs 204 (NoContent)
	if response.statusCode != http.StatusNoContent {
		//Some error occured. Compose error and return it
		err = fmt.Errorf("Queue: Error occured deleting message. Status (%s). Body:", response.statusCode, readResponseBodyAsString(response.body))
	}

	return nil
}

//DELETE https://myaccount.queue.core.windows.net/myqueue/messages
//Documentation: https://msdn.microsoft.com/en-us/library/azure/dd179454.aspx
func (q *QueueStorageClient) clearMessages(queueName string) error {
	//compose uri
	uri := q.client.getEndpoint(queueServiceName, pathForMessages(queueName), nil)
	//compose and execute request
	headers := q.client.getStandardHeaders()
	response, err := q.client.exec(HTTPDelete, uri, headers, nil)

	if err != nil {
		return err
	}

	//Success returs 204 (NoContent)
	if response.statusCode != http.StatusNoContent {
		//Some error occured. Compose error and return it
		err = fmt.Errorf("Queue: Error occured clearing messages. Status (%s). Body:", response.statusCode, readResponseBodyAsString(response.body))
	}

	return nil
}

//Reads response body as a string that can be easily printed
func readResponseBodyAsString(body io.ReadCloser) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(body)
	s := buf.String()
	return s
}

//Helper for validating message response
func getMessages(messages []QueueMessage, err error) ([]QueueMessage, error) {

	if len(messages) > 0 {
		return messages, err
	}

	return nil, err
}
