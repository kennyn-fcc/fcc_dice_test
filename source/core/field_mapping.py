class FieldMapping:
    def __init__(self, input_field: str, output_field: str, data_type: str):
        if data_type not in ["String", "Integer", "Float", "Date"]:
            raise ValueError("Data type must be one of: String, Integer, Float, Date")
        self.input_field = input_field
        self.output_field = output_field
        self.data_type = data_type