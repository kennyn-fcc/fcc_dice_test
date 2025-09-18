import os

class Config:
    def __init__(self, base_input_path: str, base_output_path: str):
        """Initialize the configuration with base paths."""
        if not self._is_valid_directory(base_input_path):
            raise ValueError(f"Invalid input directory: {base_input_path}")
        if not self._is_valid_directory(base_output_path):
            raise ValueError(f"Invalid output directory: {base_output_path}")
        self.base_input_path = base_input_path
        self.base_output_path = base_output_path

    @staticmethod
    def _is_valid_directory(path: str) -> bool:
        """Check if the given path is a valid directory."""
        return os.path.isdir(path)

    def get_input_path(self) -> str:
        """Derive the input file path for the given entity."""
        return f"{self.base_input_path}"

    def get_output_path(self) -> str:
        """Derive the output directory path for the given entity."""
        return f"{self.base_output_path}"
