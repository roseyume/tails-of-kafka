import React, { useState, useEffect } from "react";
import axios from "axios";
import { Modal, Button, Card, Container, Row, Col, ListGroup } from "react-bootstrap";
import 'bootstrap/dist/css/bootstrap.min.css';
import ConsumerModal from "./components/ConsumerModal";
import ProducerModal from "./components/ProducerModal";

function App() {
  const [topics, setTopics] = useState([]);
  const [brokers, setBrokers] = useState(0);
  const [partitions, setPartitions] = useState(0);
  const [showModal, setShowModal] = useState(false);
  const [newTopic, setNewTopic] = useState("");
  const [consumers, setConsumers] = useState([]);
  const [producers, setProducers] = useState([]);
  const [messages, setMessages] = useState({});
  const [showConsumerModal, setShowConsumerModal] = useState(false);
  const [showProducerModal, setShowProducerModal] = useState(false);
  const [editingConsumer, setEditingConsumer] = useState(null);
  const [editingProducer, setEditingProducer] = useState(null);

  const backendURL = "https://8000-roseyume-tailsofkafka-md4yrcdut1c.ws-us121.gitpod.io";

  // Fetch metrics
  const fetchMetrics = async () => {
    try {
      const [topicsRes, brokersRes, partitionsRes] = await Promise.all([
        axios.get(`${backendURL}/topics`),
        axios.get(`${backendURL}/brokers`),
        axios.get(`${backendURL}/partitions`)
      ]);
      setTopics(topicsRes.data);
      setBrokers(brokersRes.data.broker_count);
      setPartitions(partitionsRes.data.partition_count);
    } catch (err) {
      console.error(err);
    }
  };

  // Fetch consumers and producers
  const fetchConsumers = async () => {
    try {
      const res = await axios.get(`${backendURL}/consumers`);
      setConsumers(res.data);
    } catch (err) {
      console.error("Failed to fetch consumers", err);
    }
  };

  const fetchProducers = async () => {
    try {
      const res = await axios.get(`${backendURL}/producers`);
      setProducers(res.data);
    } catch (err) {
      console.error("Failed to fetch producers", err);
    }
  };


  useEffect(() => {
    fetchMetrics();
    fetchConsumers();
    fetchProducers();
    const interval = setInterval(fetchMetrics, 5000); // refresh every 5s
    return () => clearInterval(interval);
  }, []);

  // Refresh messages every 5s
  useEffect(() => {
    // Poll messages for each consumer
    const fetchMessages = async () => {
      const newMessages = {};
      for (const c of consumers) {
        try {
          const res = await axios.get(`${backendURL}/consume/${c.consumer_id}`);
          newMessages[c.consumer_id] = res.data;
        } catch (err) {
          console.error(`Failed to fetch messages for ${c.consumer_id}`, err);
        }
      }
      setMessages(newMessages);
    };
    const interval = setInterval(fetchMessages, 5000);
    return () => clearInterval(interval);
  }, [consumers]);

  // Open modals
  const openConsumerModal = (consumer = null) => {
    setEditingConsumer(consumer);
    setShowConsumerModal(true);
  };

  const openProducerModal = (producer = null) => {
    setEditingProducer(producer);
    setShowProducerModal(true);
  };

  const handleCreateTopic = async () => {
    if (!newTopic) return;
    try {
      const res = await axios.post(`${backendURL}/topics`, { name: newTopic });
      if (res.data.success) {
        setShowModal(false);
        setNewTopic("");
        fetchMetrics();
        alert(`Topic "${res.data.topic}" created!`);
      } else {
        alert(`Failed: ${res.data.error}`);
      }
    } catch (err) {
      console.error(err);
      alert("Error creating topic");
    }
  };

  return (
    <div>
    <Container className="mt-4">
      <h2>Kafka Dashboard</h2>
      <Row className="mt-3">
        <Col>
          <Card>
            <Card.Body>
              <Card.Title>Brokers</Card.Title>
              <Card.Text>{brokers}</Card.Text>
            </Card.Body>
          </Card>
        </Col>
        <Col>
          <Card>
            <Card.Body>
              <Card.Title>Partitions</Card.Title>
              <Card.Text>{partitions}</Card.Text>
            </Card.Body>
          </Card>
        </Col>
        <Col>
          <Card>
            <Card.Body>
              <Card.Title>Topics</Card.Title>
              <ul>
                {topics.map((t, idx) => <li key={idx}>{t}</li>)}
              </ul>
              <Button variant="primary" onClick={() => setShowModal(true)}>➕ Add Topic</Button>
            </Card.Body>
          </Card>
        </Col>
      </Row>

      {/* Modal */}
      <Modal show={showModal} onHide={() => setShowModal(false)}>
        <Modal.Header closeButton>
          <Modal.Title>Add New Topic</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <input
            type="text"
            className="form-control"
            placeholder="Topic name"
            value={newTopic}
            onChange={(e) => setNewTopic(e.target.value)}
          />
        </Modal.Body>
        <Modal.Footer>
          <Button variant="secondary" onClick={() => setShowModal(false)}>Cancel</Button>
          <Button variant="primary" onClick={handleCreateTopic}>Create</Button>
        </Modal.Footer>
      </Modal>
    </Container>
    <Container fluid className="p-4">
      <h2 className="mb-4">Kafka Management Dashboard</h2>

      {/* Consumers Section */}
      <Row className="mb-4">
        <Col>
          <h4>
            Consumers{" "}
            <Button size="sm" onClick={() => openConsumerModal()}>
              ➕ Add
            </Button>
          </h4>
          <Row>
            {consumers.map((c) => (
              <Col md={4} key={c.consumer_id} className="mb-3">
                <Card>
                  <Card.Body>
                    <Card.Title>
                      {c.name || c.consumer_id}
                      <Button
                        size="sm"
                        variant="outline-primary"
                        className="ms-2"
                        onClick={() => openConsumerModal(c)}
                      >
                        Edit
                      </Button>
                    </Card.Title>
                    <Card.Subtitle className="mb-2 text-muted">
                      Group: {c.group_id}
                    </Card.Subtitle>
                    <Card.Text>Topics: {c.topics?.join(", ")}</Card.Text>
                    <h6>Messages:</h6>
                    <ListGroup style={{ maxHeight: "150px", overflowY: "auto" }}>
                      {(messages[c.consumer_id] || []).map((m, i) => (
                        <ListGroup.Item key={i}>{JSON.stringify(m)}</ListGroup.Item>
                      ))}
                    </ListGroup>
                  </Card.Body>
                </Card>
              </Col>
            ))}
          </Row>
        </Col>
      </Row>

      {/* Producers Section */}
      <Row>
        <Col>
          <h4>
            Producers{" "}
            <Button size="sm" onClick={() => openProducerModal()}>
              ➕ Add
            </Button>
          </h4>
          <Row>
            {producers.map((p) => (
              <Col md={4} key={p.producer_id} className="mb-3">
                <Card>
                  <Card.Body>
                    <Card.Title>
                      {p.producer_id}
                      <Button
                        size="sm"
                        variant="outline-primary"
                        className="ms-2"
                        onClick={() => openProducerModal(p)}
                      >
                        Edit
                      </Button>
                    </Card.Title>
                    <Card.Subtitle className="mb-2 text-muted">
                      Config
                    </Card.Subtitle>
                    <ul>
                      <li>Acks: {p.acks}</li>
                      <li>Linger: {p.linger_ms} ms</li>
                      <li>Compression: {p.compression_type}</li>
                    </ul>
                  </Card.Body>
                </Card>
              </Col>
            ))}
          </Row>
        </Col>
      </Row>

      {/* Modals */}
      <ConsumerModal
        show={showConsumerModal}
        handleClose={() => setShowConsumerModal(false)}
        refreshConsumers={fetchConsumers}
        existingConsumer={editingConsumer}
      />
      <ProducerModal
        show={showProducerModal}
        handleClose={() => setShowProducerModal(false)}
        refreshProducers={fetchProducers}
        existingProducer={editingProducer}
      />
    </Container>
    </div>
  );
}

export default App;
