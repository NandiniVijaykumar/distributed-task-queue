from PIL import Image
import os
from pypdf import PdfReader
import time

def handle_resize_image(payload: dict) -> bool:
    input_path = payload.get("file")
    width = payload.get("width", 200)
    height = payload.get("height", 200)

    if not input_path or not os.path.exists(input_path):
        print(f"[handler] file not found: {input_path}")
        return False

    try:
        img = Image.open(input_path)
        img = img.resize((width, height))
        output_path = input_path.rsplit(".", 1)[0] + "_resized." + input_path.rsplit(".", 1)[1]
        img.save(output_path)
        print(f"[handler] resized {input_path} -> {output_path}")
        return True
    except Exception as e:
        print(f"[handler] resize failed: {e}")
        return False


def handle_process_pdf(payload: dict) -> bool:
    input_path = payload.get("file")
    if not input_path or not os.path.exists(input_path):
        print(f"[handler] file not found: {input_path}")
        return False

    try:
        reader = PdfReader(input_path)
        num_pages = len(reader.pages)
        print(f"[handler] processed {input_path}: {num_pages} pages")
        return True
    except Exception as e:
        print(f"[handler] pdf processing failed: {e}")
        return False

def handle_long_task(payload: dict) -> bool:
    duration = payload.get("duration_seconds", 3)
    print(f"[handler] simulating long task for {duration}s")
    time.sleep(duration)
    print(f"[handler] long task finished")
    return True

HANDLERS = {
    "resize_image": handle_resize_image,
    "process_pdf": handle_process_pdf,
    "long_task": handle_long_task,
}