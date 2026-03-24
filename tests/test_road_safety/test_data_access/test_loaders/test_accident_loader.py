import pytest
from src.road_safety.data_access.loaders import accident_loader


class TestAccidentLoader:
    def test_clean_string_value_corrects_nanterre(self):
        # Arrange
        input_val = "Non renseignee nterre"
        # Act
        result = accident_loader.clean_string_value(input_val)
        # Assert
        assert result == "Nanterre"

    def test_clean_string_value_corrects_luminosity(self):
        # Arrange
        input_val = "Nuit sanseclairage public"
        # Act
        result = accident_loader.clean_string_value(input_val)
        # Assert
        assert result == "Nuit sans eclairage public"

    def test_clean_string_value_handles_normal(self):
        # Arrange
        input_val = "Plein jour"
        # Act
        result = accident_loader.clean_string_value(input_val)
        # Assert
        assert result == "Plein jour"

    def test_safe_convert_int_valid(self):
        assert accident_loader.safe_convert_int("47") == 47

    def test_safe_convert_int_invalid(self):
        assert accident_loader.safe_convert_int("unknown") is None

    def test_prepare_data_for_insertion(self, sample_dataframe):
        # Act
        rows = accident_loader.prepare_data_for_insertion(sample_dataframe)

        # Assert
        assert len(rows) == 2
        # Verify Nanterre was corrected (second row of fixture)
        assert rows[1][3] == "Nanterre"
        # Verify luminosity was corrected
        assert rows[1][4] == "Nuit sans eclairage public"


class TestCleanStringValueEdgeCases:
    def test_non_string_passthrough_int(self):
        assert accident_loader.clean_string_value(42) == 42

    def test_non_string_passthrough_none(self):
        assert accident_loader.clean_string_value(None) is None


class TestLoadCsvData:
    def test_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError, match="File not found"):
            accident_loader.load_csv_data("/nonexistent/path/to/file.csv")

    def test_reads_valid_utf8_csv(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("commune;type_acci\nParis;Leger\n", encoding="utf-8")
        df = accident_loader.load_csv_data(str(csv_file))
        assert len(df) == 1
        assert df.iloc[0]["commune"] == "Paris"

    def test_raises_unicode_decode_error_when_all_encodings_fail(self, monkeypatch, tmp_path):
        csv_file = tmp_path / "bad.csv"
        csv_file.write_bytes(b"commune;val\ntest;1\n")

        def raise_unicode(filepath, sep, encoding, low_memory):
            raise UnicodeDecodeError(encoding, b"x", 0, 1, "bad encoding")

        monkeypatch.setattr(accident_loader.pd, "read_csv", raise_unicode)
        with pytest.raises(UnicodeDecodeError):
            accident_loader.load_csv_data(str(csv_file))


class TestPrepareDataEdgeCases:
    def test_bad_date_and_time_yield_none(self, sample_dataframe):
        df = sample_dataframe.copy()
        df.at[0, "date"] = "NOT_A_DATE"
        df.at[0, "heure"] = "NOT_A_TIME"
        rows = accident_loader.prepare_data_for_insertion(df)
        assert rows[0][1] is None   # date_acc
        assert rows[0][2] is None   # heure_acc


class TestInsertAccidents:
    def test_returns_zero_when_no_connection(self, monkeypatch):
        monkeypatch.setattr(accident_loader, "establish_connection", lambda: None)
        result = accident_loader.insert_accidents([("data",)])
        assert result == 0

    def test_successful_insertion_commits_and_returns_count(self, monkeypatch):
        committed = []

        class FakeCursor:
            rowcount = 3

            def executemany(self, sql, data):
                pass

            def close(self):
                pass

        class FakeConn:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                committed.append(True)

            def rollback(self):
                pass

            def close(self):
                pass

        monkeypatch.setattr(accident_loader, "establish_connection", lambda: FakeConn())
        result = accident_loader.insert_accidents([("data",)])
        assert result == 3
        assert committed == [True]

    def test_handles_db_exception_and_rollback(self, monkeypatch):
        rolled_back = []

        class FakeCursor:
            def executemany(self, sql, data):
                raise Exception("DB error")

        class FakeConn:
            def cursor(self):
                return FakeCursor()
            def commit(self): pass
            def rollback(self): rolled_back.append(True)
            def close(self): pass

        monkeypatch.setattr(accident_loader, "establish_connection", lambda: FakeConn())
        result = accident_loader.insert_accidents([("data",)])
        assert result == 0
        assert len(rolled_back) == 1
