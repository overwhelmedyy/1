import os
import time
import threading
import queue
import cv2
import sys
import inspect
import argparse

# Import modules
__dir__ = os.path.dirname(os.path.abspath(__file__))
sys.path.append(__dir__)
sys.path.insert(0, os.path.abspath(os.path.join(__dir__, "./3rd_library")))

from mu_ocr.main_ocr import muOCR
from mu_ocr.utils import (
    INPUT_DIR,
    OUTPUT_DIR,
    CROPPED_TABLE_FILE_NAME,
    mu_logging,
    check_file_type,
    pdf2img,
    rotate_table,
    get_supplier_path,
    get_invoice_no,
    get_supplier_cfg,
    exec_layout_handler,
    table_rslt_excel_handler,
    restruct_table_excel,
    extract_invoice_number,
    extract_origin,
    insert_invoice_no_and_currency_and_origin_ToExcel,
    match_excel_packing_list_supplier,
    generate_packing_list_result_excel
)

# gRPC imports
import grpc
import gRPC_service.service_pb2 as service_pb2
import gRPC_service.service_pb2_grpc as service_pb2_grpc
from gRPC_service.server import create_server

# Configuration
DEBUG_MODE = False
MU_MODEL_BASE_DIR = "./mu_ocr/suppliers"

# Global variables
predict_table_msg_queue = queue.Queue()
predict_table_rslt_queue = queue.Queue()
supplier_name = ""
last_supplier_name = ""
ocr_main = muOCR()

def update_model(model_type: str) -> None:
    """Update the model based on the invoice type."""
    global supplier_name, last_supplier_name  # Declare global variables

    if supplier_name == last_supplier_name:
        return

    supplier_path = get_supplier_path(supplier_name) 

    model_dirs = {
        "layout": os.path.join(MU_MODEL_BASE_DIR, supplier_path, "models", "layout"),
        "kie": os.path.join(MU_MODEL_BASE_DIR, supplier_path, "models", "ser"),
        "table": os.path.join(MU_MODEL_BASE_DIR, supplier_path, "models", "table"),
    }

    if model_type in model_dirs:
        model_dir = model_dirs[model_type]
        update_method = getattr(ocr_main, f"mu_{model_type}_model_update")
        update_method(model_dir)
        mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                   f"Updated {model_type} model for {supplier_name}: {model_dir}")

def predict_table_worker(msg_queue: queue.Queue) -> None:
    """Worker thread for predicting table."""
    while True:
        message = msg_queue.get()
        image_path = message.get('img_path')

        if image_path is None:
            mu_logging('error', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                       "Image path is None")
            continue

        try:
            if ocr_main.table_engine is None:
                mu_logging('error', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                           "Table engine is None")
                continue

            ocr_main.mu_table_predict(image_path)
            predict_table_rslt_queue.put({"ret": 0})
        except Exception as e:
            predict_table_rslt_queue.put({"ret": 1, "error": str(e)})

class OCRService(service_pb2_grpc.OCRServiceServicer):
    # def InferCN(self, request, context) -> service_pb2.OCRResponse:
    #     """gRPC method to perform OCR."""
    #     global supplier_name
    #
    #     if request.image_path is None:
    #         context.set_details("Please upload invoice")
    #         context.set_code(grpc.StatusCode.UNKNOWN)
    #         return service_pb2.OCRResponse(result_path="", supplier="", status_code=2, message="Image path is None")
    #
    #     start_time = time.time()
    #     file_path = os.path.join(INPUT_DIR, 'invoices', request.image_path)
    #     packing_list_path = os.path.join(INPUT_DIR, 'packing_list', request.packing_list_path)
    #
    #     try:
    #         # Invoice OCR
    #         rslt_excel_path = process_receipt_recognition(file_path)
    #         if not rslt_excel_path:
    #             context.set_details("mu ocr failed.")
    #             context.set_code(grpc.StatusCode.UNKNOWN)
    #             return service_pb2.OCRResponse(result_path="", packing_list_result_path="", status_code=1, message="Unsupported invoice type.")
    #
    #         # Packing list recognition
    #         rslt_packing_list_path = None
    #         if os.path.isfile(packing_list_path): #读入原始的PL，生成简略的PL
    #             rslt_packing_list_path = process_packing_list_recognition(packing_list_path)
    #             if not rslt_packing_list_path:
    #                 context.set_details("Packing list extract failed.")
    #                 context.set_code(grpc.StatusCode.UNKNOWN)
    #                 return service_pb2.OCRResponse(result_path="", packing_list_result_path="", status_code=1, message="Unsupported invoice type.")
    #
    #         mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
    #                    f"Execution time: {time.time() - start_time} seconds")
    #         return service_pb2.OCRResponse(result_path=rslt_excel_path, packing_list_result_path=rslt_packing_list_path, supplier=supplier_name, status_code=0,
    #                                        message="Success")
    #     except Exception as e:
    #         context.set_details(f"Error processing image: {str(e)}")
    #         context.set_code(grpc.StatusCode.UNKNOWN)
    #         return service_pb2.OCRResponse(result_path="", supplier="", status_code=2, message=str(e))

    def InferCN(self, request, context) -> service_pb2.OCRResponse:

        if request.file_path is None:
            context.set_details("plz upload file")
            context.set_code(grpc.StatusCode.UNKNOWN)
            return service_pb2.OCRResponse(result_path="",invoice_number="",supplier="",status_code=2,message="img path None")

        start_time = time.time()
        #TODO 待修改的路径2
        file_path = request.file_path

        if request.file_type == 0: #invoice
            rslt_excel_path, invoice_number = process_receipt_recognition(file_path)
            if not rslt_excel_path:
                context.set_details("mu ocr failed.")
                context.set_code(grpc.StatusCode.UNKNOWN)
                return service_pb2.OCRResponse(result_path="", invoice_number="", supplier="", status_code=1, message="Unsupported invoice type.")
            return service_pb2.OCRResponse(result_path=rslt_excel_path,
                                           invoice_number=invoice_number, supplier=supplier_name, status_code=0,
                                           message="Success")

        elif request.file_type == 1: #PL
            supplier = match_excel_packing_list_supplier(file_path)
            rslt_packing_list_path, invoice_number, supplier = process_packing_list_recognition(file_path)
            return service_pb2.OCRResponse(result_path=rslt_packing_list_path,
                                           invoice_number=invoice_number, supplier=supplier, status_code=0,
                                           message="Success")

        else:
            context.set_details(f"neither I or PL")
            context.set_code(grpc.StatusCode.UNKNOWN)
            return service_pb2.OCRResponse(result_path="", invoice_number="", supplier="", status_code=2, message="str(e)")




def process_receipt_recognition(file_path: str):
    """Process receipt recognition."""
    global supplier_name, last_supplier_name  # Declare global variables

    try:
        # 判断文件类型，如果是PDF需要转成IMG
        img_path= None
        file_type = check_file_type(file_path)
        if file_type == "PDF":
            img_path = pdf2img(file_path)
        elif file_type == "IMG":
            img_path = file_path
        else:
            mu_logging('error', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                    "Unsupported file type")
            return None
        
        # 读取图像
        if not img_path:
            raise ValueError("File not found or unable to read.")
        img = cv2.imread(img_path)

        # ----------------------整体OCR匹配供应商---------------------------
        ocr_main.mu_ocr_infer(img)
        supplier_name = ocr_main.match_supplier()
        mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                    f"Matched supplier: {supplier_name}")

        if not supplier_name:
            mu_logging('error', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                       "No supplier matched")
            return None 
        # ----------------------图像预处理：图像矫正------------------------
        img = rotate_table(img)

        # ----------------------支持的票据类型，开始处理---------------------
        supplier_config = get_supplier_cfg(supplier_name)
        mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                   f"Supplier config: {supplier_config}")

        # ------------------------版面分析和表格识别------------------------
        # 删除前序的表格截取文件
        if os.path.exists(ocr_main.cropped_table_image_path):
            os.remove(ocr_main.cropped_table_image_path)
        
        # 提取表格
        if supplier_config['invoice']['layout']:
            # 版面分析
            update_model("layout")
            ocr_main.mu_layout_infer(img)
        else:
            # 直接根据供应商定制截取表格， 写cropped_table_img
            exec_layout_handler(file_path, supplier_config)

        # 表格识别
        if os.path.exists(ocr_main.cropped_table_image_path):
            update_model("table")
            table_predict_message = {'img_path': ocr_main.cropped_table_image_path}
            predict_table_msg_queue.put(table_predict_message)
            result = predict_table_rslt_queue.get()  # 阻塞，直到有结果返回
            mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                       f"Main thread received: {result}")
        else:
            mu_logging('error', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                       "Extract table fail")
            return None

        table_rslt_excel_path = os.path.join(OUTPUT_DIR, f"{os.path.basename(CROPPED_TABLE_FILE_NAME).split('.')[0]}.xlsx")
        if os.path.exists(table_rslt_excel_path):
            # 供应商定制化识别结果excel结果处理
            if supplier_config['invoice']['table_rslt_excel_handler']:
                table_rslt_excel_handler(table_rslt_excel_path, supplier_config)
            # 重组表格识别结果excel,表格变成中文了
            restruct_table_excel(supplier_name)
        else:
            mu_logging('error', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                       "Table recognition fail")
            return None

        # -----------关键信息抽取KIE(Key Information Extraction)-----------
        invoice_no = ""
        if supplier_config['invoice']['kie']:
            update_model("kie")
            result_dict = ocr_main.mu_kie_infer(img_path)
            mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                       f"KIE result: {result_dict}")

            if result_dict is None:
                mu_logging('error', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                           "KIE recognition fail")
                return None

            invoice_no = get_invoice_no(result_dict)
        else:
            # print(ocr_main.receipt_ocr_content)
            invoice_no = extract_invoice_number(supplier_config, ocr_main.receipt_ocr_content)

        origin = ""
        if supplier_config['invoice']['origin']:       #  原产国
            origin = extract_origin(supplier_config, ocr_main.receipt_ocr_content) # r_o_c 是ocr识别的结果
        
        # 将发票号、币别抽取结果插入表格识别结果excel中
        insert_invoice_no_and_currency_and_origin_ToExcel(CROPPED_TABLE_FILE_NAME, OUTPUT_DIR, invoice_no, origin, supplier_name, img_path)

        # --------------保存供应商类型用于下一次判断是否更新模型-------------
        last_supplier_name = supplier_name

        # ----------------------返回识别结果excel路径----------------------
        rslt_excel_path = os.path.join(OUTPUT_DIR, f"{os.path.basename(img_path).split('.')[0]}.xlsx")
        return (rslt_excel_path, invoice_no) if os.path.exists(rslt_excel_path) else None
    except Exception as e:
        mu_logging('error', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                    f"Error processing invoice {file_path}: {str(e)}")
        return None  

def process_packing_list_recognition(packing_list_path: str):
    """Process packing list recognition."""
    try:
        supplier = None
        if packing_list_path.endswith(('.xlsx', '.xls')):
            supplier = match_excel_packing_list_supplier(packing_list_path)

        if supplier is not None:
            mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                       f"Packing list corresponding supplier: {supplier['name']}")
            return (generate_packing_list_result_excel(packing_list_path, supplier), supplier)

        return None
    except Exception as e:
        mu_logging('error', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                   f"Error processing packing list {packing_list_path}: {str(e)}")
        return None  

if __name__ == "__main__":
    # 启动表格识别线程
    worker_thread = threading.Thread(target=predict_table_worker, args=(predict_table_msg_queue,))
    worker_thread.start()

    if DEBUG_MODE:
        parser = argparse.ArgumentParser(description="muVision")
        parser.add_argument("input_path", help="Path to the input PDF/Image file or directory")
        args = parser.parse_args()

        start_time = time.time()
        process_receipt_recognition(args.input_path)
        mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                   f"Program execution time: {time.time() - start_time} seconds")
    else:
        server = create_server()
        service_pb2_grpc.add_OCRServiceServicer_to_server(OCRService(), server)
        server.start()
        mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                   "gRPC server running at port: 50051")

        try:
            while True:
                time.sleep(86400)
        except KeyboardInterrupt:
            mu_logging('info', os.path.basename(__file__), inspect.currentframe().f_code.co_name, inspect.currentframe().f_lineno,
                       "muVision OCR closing...")
            server.stop(0)
