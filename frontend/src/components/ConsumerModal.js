import React, { useState, useEffect } from "react";
import axios from "axios";
import { Modal, Button, Form } from "react-bootstrap";

function ConsumerModal({ show, handleClose, refreshConsumers, existingConsumer }) {
    const [consumerId, setConsumerId] = useState("");
    const [name, setName] = useState("");
    const [groupId, setGroupId] = useState("");
    const [topics, setTopics] = useState("");
    const [autoCommit, setAutoCommit] = useState(true);
    const [offsetReset, setOffsetReset] = useState("latest");
    const [maxPollRecords, setMaxPollRecords] = useState(500);
    const [isolationLevel, setIsolationLevel] = useState("read_uncommitted");

    useEffect(() => {
        if (existingConsumer) {
            setConsumerId(existingConsumer.consumer_id);
            setGroupId(existingConsumer.group_id);
            setTopics(existingConsumer.topics.join(","));
            setAutoCommit(existingConsumer.enable_auto_commit);
            setOffsetReset(existingConsumer.auto_offset_reset);
            setMaxPollRecords(existingConsumer.max_poll_records || 500);
            setIsolationLevel(existingConsumer.isolation_level || "read_uncommitted");
        }
    }, [existingConsumer]);

    const handleSubmit = async () => {
        const payload = {
            consumer_id: consumerId,
            group_id: groupId,
            topics: topics.split(",").map(t => t.trim()),
            enable_auto_commit: autoCommit,
            auto_offset_reset: offsetReset,
            max_poll_records: maxPollRecords,
            isolation_level: isolationLevel
        };

        try {
            const url = existingConsumer
                ? `http://localhost:8000/update_consumer/${consumerId}`
                : "http://localhost:8000/configure_consumer";

            const res = await axios.post(url, payload).catch(async e => {
                if (existingConsumer) {
                    return await axios.put(url, payload);
                }
                throw e;
            });

            if (res.data.success) {
                alert(`Consumer ${consumerId} configured!`);
                handleClose();
                refreshConsumers();
            } else {
                alert(`Error: ${res.data.error}`);
            }
        } catch (err) {
            console.error(err);
            alert("Failed to configure consumer");
        }
    };

    return (
        <Modal show={show} onHide={handleClose}>
            <Modal.Header closeButton>
                <Modal.Title>{existingConsumer ? "Update" : "Create"} Consumer</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <Form>
                    <Form.Group className="mb-2">
                        <Form.Label>Consumer Name</Form.Label>
                        <Form.Control
                            value={name}
                            onChange={e => setName(e.target.value)}
                        />
                    </Form.Group>
                    <Form.Group className="mb-2">
                        <Form.Label>Consumer ID</Form.Label>
                        <Form.Control
                            value={consumerId}
                            onChange={e => setConsumerId(e.target.value)}
                            disabled={!!existingConsumer} // cannot change ID when updating
                        />
                    </Form.Group>
                    <Form.Group className="mb-2">
                        <Form.Label>Group ID</Form.Label>
                        <Form.Control value={groupId} onChange={e => setGroupId(e.target.value)} />
                    </Form.Group>
                    <Form.Group className="mb-2">
                        <Form.Label>Topics (comma separated)</Form.Label>
                        <Form.Control value={topics} onChange={e => setTopics(e.target.value)} />
                    </Form.Group>
                    <Form.Group className="mb-2">
                        <Form.Check
                            type="checkbox"
                            label="Enable Auto Commit"
                            checked={autoCommit}
                            onChange={e => setAutoCommit(e.target.checked)}
                        />
                    </Form.Group>
                    <Form.Group className="mb-2">
                        <Form.Label>Auto Offset Reset</Form.Label>
                        <Form.Select value={offsetReset} onChange={e => setOffsetReset(e.target.value)}>
                            <option value="latest">latest</option>
                            <option value="earliest">earliest</option>
                        </Form.Select>
                    </Form.Group>
                    <Form.Group className="mb-2">
                        <Form.Label>Max Poll Records</Form.Label>
                        <Form.Control
                            type="number"
                            value={maxPollRecords}
                            onChange={e => setMaxPollRecords(Number(e.target.value))}
                        />
                    </Form.Group>
                    <Form.Group className="mb-2">
                        <Form.Label>Isolation Level</Form.Label>
                        <Form.Select
                            value={isolationLevel}
                            onChange={e => setIsolationLevel(e.target.value)}
                        >
                            <option value="read_uncommitted">read_uncommitted</option>
                            <option value="read_committed">read_committed</option>
                        </Form.Select>
                    </Form.Group>
                </Form>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={handleClose}>Cancel</Button>
                <Button variant="primary" onClick={handleSubmit}>
                    {existingConsumer ? "Update" : "Create"} Consumer
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

export default ConsumerModal;
