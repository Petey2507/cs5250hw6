import unittest
from unittest import mock
from botocore.exceptions import ClientError
import sys

sys.argv = ["test_consumer.py", "--storage", "s3", "--bucket", "dummy-bucket"]

from consumer import (
    get_widget_request,
    check_schema,
    s3_store,
    dynamodb_store,
    execute_request,
    s3_delete,
    dynamodb_delete,
    s3_update,
    dynamodb_update,
)

class TestGetWidgetRequest(unittest.TestCase):
    @mock.patch('consumer.s3_client')
    def test_get_widget_request_success(self, mock_s3):
        mock_s3.list_objects_v2.return_value = {'Contents': [{'Key': 'some-key'}]}
        mock_s3.get_object.return_value = {'Body': mock.Mock(read=lambda: b'{"type": "create"}')}
        result, key = get_widget_request('dummy-bucket')
        self.assertIsNotNone(result)
        self.assertEqual(key, 'some-key')

    @mock.patch('consumer.s3_client')
    def test_get_widget_request_no_objects(self, mock_s3):
        mock_s3.list_objects_v2.return_value = {}
        result, key = get_widget_request('dummy-bucket')
        self.assertIsNone(result)
        self.assertIsNone(key)

    @mock.patch('consumer.s3_client')
    def test_get_widget_request_client_error(self, mock_s3):
        mock_s3.list_objects_v2.side_effect = ClientError(
            error_response={'Error': {'Code': '500', 'Message': 'Error'}},
            operation_name='ListObjectsV2'
        )
        result, key = get_widget_request('dummy-bucket')
        self.assertIsNone(result)
        self.assertIsNone(key)

class TestCheckSchema(unittest.TestCase):
    def test_check_schema_valid(self):
        valid_data = {"type": "create", "requestId": "123", "widgetId": "456", "owner": "user"}
        try:
            check_schema(valid_data)
        except ValueError:
            self.fail("check_schema failed")

    def test_check_schema_missing_fields(self):
        invalid_data = {"type": "create", "requestId": "123"}
        with self.assertRaises(ValueError):
            check_schema(invalid_data)

    def test_check_schema_invalid_type(self):
        invalid_data = {"type": "invalid", "requestId": "123", "widgetId": "456", "owner": "user"}
        with self.assertRaises(ValueError):
            check_schema(invalid_data)

class TestStorageFunctions(unittest.TestCase):
    @mock.patch('consumer.s3_client')
    def test_s3_store(self, mock_s3):
        widget_data = {"owner": "user", "widgetId": "widget1"}
        s3_store(widget_data, 'dummy-bucket')
        mock_s3.put_object.assert_called_once()

    @mock.patch('consumer.dynamodb_client.Table')
    def test_dynamodb_store(self, mock_table):
        mock_table_instance = mock_table.return_value
        widget_data = {"owner": "user", "widgetId": "widget1"}
        dynamodb_store(widget_data, mock_table_instance)
        mock_table_instance.put_item.assert_called_once_with(
            Item={
                "id": widget_data["widgetId"],
                "owner": widget_data["owner"],
                "label": "",
                "description": ""
            }
        )

    @mock.patch('consumer.s3_client')
    def test_s3_delete(self, mock_s3):
        widget_data = {"owner": "user", "widgetId": "widget1"}
        s3_delete(widget_data, 'dummy-bucket')
        mock_s3.delete_object.assert_called_once_with(
            Bucket='dummy-bucket',
            Key='widgets/user/widget1.json'
        )

    @mock.patch('consumer.dynamodb_client.Table')
    def test_dynamodb_delete(self, mock_table):
        mock_table_instance = mock_table.return_value
        widget_data = {"widgetId": "widget1"}
        dynamodb_delete(widget_data, mock_table_instance)
        mock_table_instance.delete_item.assert_called_once_with(Key={"id": "widget1"})

    @mock.patch('consumer.s3_client')
    def test_s3_update(self, mock_s3):
        widget_data = {"owner": "user", "widgetId": "widget1"}
        s3_update(widget_data, 'dummy-bucket')
        mock_s3.put_object.assert_called_once_with(
            Bucket='dummy-bucket',
            Key='widgets/user/widget1.json',
            Body=mock.ANY
        )

    @mock.patch('consumer.dynamodb_client.Table')
    def test_dynamodb_update(self, mock_table):
        mock_table_instance = mock_table.return_value
        widget_data = {"widgetId": "widget1", "owner": "user", "label": "new label"}
        dynamodb_update(widget_data, mock_table_instance)
        mock_table_instance.update_item.assert_called_once_with(
            Key={"id": "widget1"},
            UpdateExpression="SET #owner=:owner, #label=:label",
            ExpressionAttributeNames={"#owner": "owner", "#label": "label"},
            ExpressionAttributeValues={":owner": "user", ":label": "new label"}
        )

class TestExecuteRequest(unittest.TestCase):
    @mock.patch('consumer.create_request_handle')
    def test_execute_request_create(self, mock_create):
        req_data = {"type": "create", "widgetId": "widget1", "owner": "owner"}
        execute_request(req_data)
        mock_create.assert_called_once_with(req_data)

    @mock.patch('consumer.delete_request_handle')
    def test_execute_request_delete(self, mock_delete):
        req_data = {"type": "delete", "widgetId": "widget1"}
        execute_request(req_data)
        mock_delete.assert_called_once_with(req_data)

    @mock.patch('consumer.update_request_handle')
    def test_execute_request_update(self, mock_update):
        req_data = {"type": "update", "widgetId": "widget1"}
        execute_request(req_data)
        mock_update.assert_called_once_with(req_data)

if __name__ == '__main__':
    unittest.main()
