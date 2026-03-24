from unittest.mock import patch, MagicMock
from road_safety.runners import accident_explorer


class TestAccidentExplorer:
    @patch("src.road_safety.runners.accident_explorer.accident_loader")
    @patch("src.road_safety.runners.accident_explorer.accident_table")
    def test_execute_analysis_success(self, mock_table, mock_loader):
        # Arrange
        mock_table.create_accident_table.return_value = True
        mock_loader.load_csv_data.return_value = MagicMock()
        mock_loader.prepare_data_for_insertion.return_value = []
        mock_loader.insert_accidents.return_value = 10

        # Act
        accident_explorer.execute_analysis()

        # Assert
        mock_table.create_accident_table.assert_called_once()
        mock_loader.load_csv_data.assert_called_once()
        mock_loader.insert_accidents.assert_called_once()

    @patch("src.road_safety.runners.accident_explorer.accident_loader")
    @patch("src.road_safety.runners.accident_explorer.accident_table")
    def test_execute_analysis_handles_file_not_found(
        self, mock_table, mock_loader, capsys
    ):
        mock_table.create_accident_table.return_value = None
        mock_loader.load_csv_data.side_effect = FileNotFoundError("no csv")

        accident_explorer.execute_analysis()

        out = capsys.readouterr().out
        assert "not found" in out.lower()

    @patch("src.road_safety.runners.accident_explorer.accident_loader")
    @patch("src.road_safety.runners.accident_explorer.accident_table")
    def test_execute_analysis_handles_general_exception(
        self, mock_table, mock_loader, capsys
    ):
        mock_table.create_accident_table.return_value = None
        mock_loader.load_csv_data.side_effect = ValueError("unexpected error")

        accident_explorer.execute_analysis()

        out = capsys.readouterr().out
        assert "error occurred" in out.lower()
