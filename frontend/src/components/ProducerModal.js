import React, { useState, useEffect } from "react";
import axios from "axios";
import { Modal, Button, Form } from "react-bootstrap";

function ProducerModal({ show, handleClose, refreshProducers, existingProducer }) {
  const [producerId, setProducerId] = useState("");
  const [acks, setAcks] = useState("all");
  const [lingerMs, setLingerMs] = useState(0);
  const [compression, setCompression] = useState("none");

  useEffect(() => {
    if (existingProducer) {
      setProducerId(existingProducer.producer_id);
      setAcks(existingProducer.acks || "all");
      setLingerMs(existingProducer.linger_ms || 0);
      setCompression(existingProducer.compression_type || "none");
    }
  }, [existingProducer]);

  const handleSubmit = async () => {
    const payload = {
      producer_id: producerId,
      acks,
      linger_ms: lingerMs,
      compression_type: compression
    };

    try {
      const url = existingProducer
        ? `http://localhost:8000/update_producer/${producerId}`
        : "http://localhost:8000/configure_producer";

      const res = await axios.post(url, payload).catch(async e => {
        if (existingProducer) {
          return await axios.put(url, payload);
        }
        throw e;
      });

      if (res.data.success) {
        alert(`Producer ${producerId} configured!`);
        handleClose();
        refreshProducers();
      } else {
        alert(`Error: ${res.data.error}`);
      }
    } catch (err) {
      console.error(err);
      alert("Failed to configure producer");
    }
  };

  return (
    <Modal show={show} onHide={handleClose}>
      <Modal.Header closeButton>
        <Modal.Title>{existingProducer ? "Update" : "Create"} Producer</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        <Form>
          <Form.Group className="mb-2">
            <Form.Label>Producer ID</Form.Label>
            <Form.Control
              value={producerId}
              onChange={e => setProducerId(e.target.value)}
              disabled={!!existingProducer}
            />
          </Form.Group>
          <Form.Group className="mb-2">
            <Form.Label>Acks</Form.Label>
            <Form.Select value={acks} onChange={e => setAcks(e.target.value)}>
              <option value="all">all</option>
              <option value="1">1</option>
              <option value="0">0</option>
            </Form.Select>
          </Form.Group>
          <Form.Group className="mb-2">
            <Form.Label>Linger ms</Form.Label>
            <Form.Control
              type="number"
              value={lingerMs}
              onChange={e => setLingerMs(Number(e.target.value))}
            />
          </Form.Group>
          <Form.Group className="mb-2">
            <Form.Label>Compression Type</Form.Label>
            <Form.Select value={compression} onChange={e => setCompression(e.target.value)}>
              <option value="none">none</option>
              <option value="gzip">gzip</option>
              <option value="snappy">snappy</option>
              <option value="lz4">lz4</option>
            </Form.Select>
          </Form.Group>
        </Form>
      </Modal.Body>
      <Modal.Footer>
        <Button variant="secondary" onClick={handleClose}>Cancel</Button>
        <Button variant="primary" onClick={handleSubmit}>
          {existingProducer ? "Update" : "Create"} Producer
        </Button>
      </Modal.Footer>
    </Modal>
  );
}

export default ProducerModal;
