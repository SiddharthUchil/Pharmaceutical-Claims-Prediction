functions = [
    {
        "name": "create_appointment",
        "description": "Create a appointment for the customer",
        "parameters": {
            "type": "object",
            "properties": {
                "appointment_text": {
                    "type": "string",
                    "description": "The text of the appointment, e.g. Sure your appointment is booked for date ",
                },
            },
            "required": ["appointment_text"],
        },
    },
    {
        "name": "ask_vector_db",
        "description": "Ask any question related to our bank. This can include queries about our opening hours, range of banking accounts and different services offered",
        "parameters": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "What are the different types of bank accounts offered by Scotia Bank?'",
                },
            },
            "required": ["question"],
        },
    },
]
