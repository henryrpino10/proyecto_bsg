"""
Tests unitarios para el módulo detector.
"""

import pytest
import numpy as np
from unittest.mock import Mock, patch
from classification_system.detector import ObjectDetector


class TestObjectDetector:
    """Tests para ObjectDetector."""

    @pytest.fixture
    def mock_yolo_model(self):
        """Fixture para modelo YOLO mockeado."""
        with patch('classification_system.detector.YOLO') as mock:
            mock_instance = Mock()
            mock_instance.names = {0: "person", 1: "car", 2: "dog"}
            mock.return_value = mock_instance
            yield mock_instance

    @pytest.fixture
    def detector(self, mock_yolo_model):
        """Fixture para detector."""
        return ObjectDetector(model_path="yolov8n.pt", confidence=0.25)

    def test_detector_initialization(self, detector):
        """Test que el detector se inicializa correctamente."""
        assert detector.confidence == 0.25
        assert detector.model is not None

    def test_generate_detection_id(self):
        """Test generación de ID único."""
        source_info = {
            "source": "test.mp4",
            "frame_number": 10,
            "timestamp": "2024-01-01T00:00:00",
        }

        id1 = ObjectDetector._generate_detection_id(source_info, 0)
        id2 = ObjectDetector._generate_detection_id(source_info, 1)

        # IDs deben ser diferentes para diferentes índices
        assert id1 != id2
        assert len(id1) == 32  # MD5 hash length

    def test_extract_attributes_structure(self, detector):
        """Test que _extract_attributes retorna la estructura correcta."""
        # Mock box object
        mock_box = Mock()
        mock_box.xyxy = [Mock()]
        mock_box.xyxy[0].cpu().numpy.return_value = np.array([10, 20, 50, 80])
        mock_box.cls = [Mock()]
        mock_box.cls[0].cpu().numpy.return_value = 0
        mock_box.conf = [Mock()]
        mock_box.conf[0].cpu().numpy.return_value = 0.95

        image_shape = (480, 640, 3)
        source_info = {
            "type": "image",
            "source": "test.jpg",
            "timestamp": "2024-01-01T00:00:00",
        }

        detection = detector._extract_attributes(
            box=mock_box,
            image_shape=image_shape,
            source_info=source_info,
            detection_idx=0,
        )

        # Verificar campos requeridos
        required_fields = [
            "detection_id",
            "class_name",
            "confidence",
            "bbox_x1",
            "bbox_y1",
            "bbox_x2",
            "bbox_y2",
            "bbox_width",
            "bbox_height",
            "center_x",
            "center_y",
        ]

        for field in required_fields:
            assert field in detection

        # Verificar valores
        assert detection["class_name"] == "person"
        assert detection["confidence"] == 0.95
        assert detection["bbox_width"] == 40
        assert detection["bbox_height"] == 60

    def test_get_model_info(self, detector):
        """Test que get_model_info retorna información correcta."""
        info = detector.get_model_info()

        assert "num_classes" in info
        assert "class_names" in info
        assert "confidence_threshold" in info
        assert info["confidence_threshold"] == 0.25


@pytest.mark.parametrize(
    "confidence,expected",
    [
        (0.25, 0.25),
        (0.5, 0.5),
        (0.9, 0.9),
    ],
)
def test_detector_confidence_levels(confidence, expected):
    """Test diferentes niveles de confianza."""
    with patch('classification_system.detector.YOLO'):
        detector = ObjectDetector(confidence=confidence)
        assert detector.confidence == expected