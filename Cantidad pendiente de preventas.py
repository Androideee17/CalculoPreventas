import os
import requests
import pandas as pd
import logging
import json
from decimal import Decimal
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import unicodedata

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Cargar variables de entorno desde .env
load_dotenv()

app = Flask(__name__)

SHOPIFY_URL = os.getenv("SHOPIFY_URL")
SHOPIFY_API_TOKEN = os.getenv("SHOPIFY_API_TOKEN")

###############################################################################
# 1. FUNCIONES AUXILIARES
###############################################################################

def normalizar_cadena(texto):
    """
    Convierte la cadena a minúsculas, elimina tildes y espacios sobrantes.
    """
    if not texto:
        return ""
    texto = texto.strip().lower()
    texto = ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )
    return texto

def obtener_pedido(order_id):
    """
    Retorna el objeto JSON de un pedido de Shopify usando su order_id.
    """
    url = f"https://{SHOPIFY_URL}/admin/api/2023-10/orders/{order_id}.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()["order"]

def obtener_constante_producto(product_id):
    """
    Obtiene el metafield 'constante' (namespace=custom, key=constante) de tipo money.
    Si el valor está en formato JSON, se extrae 'amount'. Retorna 0 si no existe.
    """
    url = (
        f"https://{SHOPIFY_URL}/admin/api/2023-10/products/{product_id}/"
        "metafields.json?namespace=custom&key=constante"
    )
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    metafields = resp.json().get("metafields", [])
    if metafields:
        valor_raw = metafields[0]["value"]
        try:
            # Por si la value se guardó como JSON {"amount":"500.00","currency_code":"MXN"}
            valor_json = json.loads(valor_raw)  
            valor = float(valor_json.get("amount", 0))
        except Exception:
            # Si no es JSON, tomamos el valor tal cual
            valor = float(valor_raw)
        logging.info(f"Producto {product_id}: Obtenido 'constante' = {valor}")
        return valor

    logging.info(f"Producto {product_id}: No se encontró metafield 'constante'")
    return 0.0

def obtener_tarifa_local(
    peso_kg,
    estado,
    archivo_csv="envios_pendientes - Hoja 1.csv"
):
    """
    Retorna (tarifa, paqueteria) según un archivo CSV local.
    - 'tarifa' (float)
    - 'paqueteria' (str)

    1) Normaliza el nombre de 'ubicacion' del CSV y el 'estado' ingresado.
    2) Hace un filtro parcial (p.ej. 'guerrero' -> 'Estado de Guerrero').
    3) Luego toma la fila con peso_kg >= peso_kg y retorna la primera tarifa.
    """
    df = pd.read_csv(archivo_csv)
    df["ubicacion_normalizada"] = df["ubicacion"].apply(normalizar_cadena)
    estado_normalizado = normalizar_cadena(estado)

    df_match = df[df["ubicacion_normalizada"].str.contains(estado_normalizado, na=False)]
    if df_match.empty:
        logging.info(f"Para peso {peso_kg}kg y estado '{estado}', no se encontró tarifa aplicable.")
        return 0.0, ""

    df_aplicable = df_match[df_match["peso_kg"] >= peso_kg].sort_values("peso_kg")
    if not df_aplicable.empty:
        row = df_aplicable.iloc[0]
        tarifa = float(row["tarifa"])
        paqueteria = str(row["paqueteria"])
        logging.info(
            f"Para peso {peso_kg}kg y estado '{estado}' (normalizado='{estado_normalizado}'), "
            f"tarifa={tarifa}, paqueteria={paqueteria}"
        )
        return tarifa, paqueteria

    logging.info(f"Para peso {peso_kg}kg y estado '{estado}' no se encontró tarifa por peso.")
    return 0.0, ""

def guardar_metafield_pedido_money(order_id, key, value):
    """
    Crea o actualiza un metafield de tipo money en el pedido.
      namespace="custom"
      key -> p.ej. "cantidad_pendiente_productos"

    Valor se envía como JSON: {"amount": "X.YY", "currency_code": "MXN"}
    """
    currency_code = "MXN"
    valor_str = f"{Decimal(value):.2f}"
    value_json_str = json.dumps({"amount": valor_str, "currency_code": currency_code})

    url = f"https://{SHOPIFY_URL}/admin/api/2023-10/orders/{order_id}/metafields.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "metafield": {
            "namespace": "custom",
            "key": key,
            "value": value_json_str,
            "type": "money"
        }
    }
    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 422 and "already exists" in resp.text:
        # Actualizar si ya existe
        existing_mf_url = f"{url}?namespace=custom&key={key}"
        existing_mf_resp = requests.get(existing_mf_url, headers=headers)
        existing_metafields = existing_mf_resp.json().get("metafields", [])
        if existing_metafields:
            metafield_id = existing_metafields[0]["id"]
            update_url = f"https://{SHOPIFY_URL}/admin/api/2023-10/metafields/{metafield_id}.json"
            upd_resp = requests.put(update_url, headers=headers, json=payload)
            upd_resp.raise_for_status()
            logging.info(f"Pedido {order_id}: Metafield '{key}' actualizado a {value_json_str}")
            return
    else:
        # Si no es 422 o no coincide con "already exists", forzamos el raise_for_status()
        resp.raise_for_status()
    logging.info(f"Pedido {order_id}: Metafield '{key}' configurado a {value_json_str}")

def guardar_metafield_pedido_text(order_id, key, value):
    """
    Crea o actualiza un metafield de tipo texto (single_line_text_field) en el pedido.
      namespace="custom"
      key -> p.ej. "paqueteria_"

    NOTA: Si tu definición de metafield en el Admin no permite cadenas vacías,
          puedes evitar guardarlo si 'value' está vacío.
    """
    # ----------------------------------------------
    # EJEMPLO: SALTARSE GUARDADO SI ES EMPTY
    # ----------------------------------------------
    # if not value.strip():
    #     logging.info(f"Pedido {order_id}: Valor 'paqueteria_' está vacío, se omite guardado")
    #     return

    url = f"https://{SHOPIFY_URL}/admin/api/2023-10/orders/{order_id}/metafields.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
        "Content-Type": "application/json"
    }
    payload = {
        "metafield": {
            "namespace": "custom",
            "key": key,
            "value": str(value),
            "type": "single_line_text_field"
        }
    }

    resp = requests.post(url, headers=headers, json=payload)
    if resp.status_code == 422 and "already exists" in resp.text:
        # Si el metafield ya existe, lo actualizamos
        existing_mf_url = f"{url}?namespace=custom&key={key}"
        existing_mf_resp = requests.get(existing_mf_url, headers=headers)
        existing_metafields = existing_mf_resp.json().get("metafields", [])
        if existing_metafields:
            metafield_id = existing_metafields[0]["id"]
            update_url = f"https://{SHOPIFY_URL}/admin/api/2023-10/metafields/{metafield_id}.json"
            upd_resp = requests.put(update_url, headers=headers, json=payload)
            upd_resp.raise_for_status()
            logging.info(f"Pedido {order_id}: Metafield (texto) '{key}' actualizado a '{value}'")
            return
    else:
        resp.raise_for_status()
    logging.info(f"Pedido {order_id}: Metafield (texto) '{key}' configurado a '{value}'")

###############################################################################
# 2. ENDPOINT WEBHOOK
###############################################################################

@app.route("/webhook/order_created", methods=["POST"])
def webhook_order_created():
    data = request.get_json()
    if not data:
        logging.error("Webhook sin datos JSON")
        return jsonify({"error": "No JSON data received"}), 400

    order = data.get("order") or data
    order_id = order["id"]
    line_items = order.get("line_items", [])
    shipping_lines = order.get("shipping_lines", [])
    shipping_address = order.get("shipping_address", {})

    # 1. Calcular 'cantidad_pendiente_productos'
    cantidad_pendiente_productos = 0.0
    for item in line_items:
        product_id = item["product_id"]
        quantity = item["quantity"]
        prod_url = f"https://{SHOPIFY_URL}/admin/api/2023-10/products/{product_id}.json?fields=tags"
        headers = {
            "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
            "Content-Type": "application/json"
        }
        resp_prod = requests.get(prod_url, headers=headers)
        resp_prod.raise_for_status()
        product_tags = resp_prod.json()["product"]["tags"].split(",")

        if "yo" in [t.strip() for t in product_tags]:
            constante = obtener_constante_producto(product_id)
            subtotal = constante * quantity
            cantidad_pendiente_productos += subtotal
            logging.info(
                f"Pedido {order_id}: Producto {product_id} (qty {quantity}) suma {subtotal} a cantidad pendiente"
            )

    # 2. Determinar si incluye "preventa"
    envio_pendiente = 0.0
    paqueteria = ""
    has_preventa = any("preventa" in sl.get("title", "").lower() for sl in shipping_lines)

    if has_preventa:
        peso_total_kg = float(order.get("total_weight", 0)) / 1000.0
        estado = shipping_address.get("province", "") or ""
        envio_pendiente, paqueteria = obtener_tarifa_local(peso_total_kg, estado)
    else:
        logging.info(
            f"Pedido {order_id}: Ningún shipping line contiene 'preventa', "
            "no se calcula ni guarda costo de envío."
        )

    # 3. Calcular 'pendiente_pago' (productos + envío)
    total_pendiente = cantidad_pendiente_productos + envio_pendiente

    # 4. Guardar los metafields
    try:
        guardar_metafield_pedido_money(order_id, "cantidad_pendiente_productos", cantidad_pendiente_productos)
        guardar_metafield_pedido_money(order_id, "envio_pendiente", envio_pendiente)
        guardar_metafield_pedido_money(order_id, "pendiente_pago", total_pendiente)
        guardar_metafield_pedido_text(order_id, "paqueteria_", paqueteria)
        logging.info(
            f"Pedido {order_id}: Metafields guardados. Productos={cantidad_pendiente_productos}, "
            f"Envío={envio_pendiente}, Total={total_pendiente}, Paqueteria='{paqueteria}'"
        )

    except requests.exceptions.HTTPError as http_err:
        # Leer detalle de error
        logging.error(
            f"Pedido {order_id}: Error al guardar metafields (HTTPError) - {http_err} "
            f"Response Body: {http_err.response.text}"
        )
        return jsonify({"error": str(http_err), "response_body": http_err.response.text}), 500
    except Exception as e:
        logging.error(f"Pedido {order_id}: Error al guardar metafields - {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "status": "Metafields actualizados",
        "order_id": order_id,
        "cantidad_pendiente_productos": cantidad_pendiente_productos,
        "envio_pendiente": envio_pendiente,
        "pendiente_pago": total_pendiente,
        "paqueteria": paqueteria
    }), 200

###############################################################################
# 3. ENDPOINT MANUAL (similar lógica)
###############################################################################

@app.route("/actualizar_pedido/<int:order_id>", methods=["GET"])
def actualizar_pedido_manual(order_id):
    try:
        order = obtener_pedido(order_id)
    except Exception as e:
        logging.error(f"Pedido {order_id}: No se pudo obtener - {e}")
        return jsonify({"error": "No se pudo obtener el pedido"}), 400

    line_items = order.get("line_items", [])
    shipping_lines = order.get("shipping_lines", [])
    shipping_address = order.get("shipping_address", {})

    cantidad_pendiente_productos = 0.0
    for item in line_items:
        product_id = item["product_id"]
        quantity = item["quantity"]
        headers = {
            "X-Shopify-Access-Token": SHOPIFY_API_TOKEN,
            "Content-Type": "application/json"
        }
        prod_url = f"https://{SHOPIFY_URL}/admin/api/2023-10/products/{product_id}.json?fields=tags"
        resp_prod = requests.get(prod_url, headers=headers)
        resp_prod.raise_for_status()
        product_tags = resp_prod.json()["product"]["tags"].split(",")

        if "yo" in [t.strip() for t in product_tags]:
            constante = obtener_constante_producto(product_id)
            cantidad_pendiente_productos += (constante * quantity)

    # Determinar si se incluye 'preventa'
    has_preventa = any("preventa" in sl.get("title", "").lower() for sl in shipping_lines)

    envio_pendiente = 0.0
    paqueteria = ""
    if has_preventa:
        peso_total_kg = float(order.get("total_weight", 0)) / 1000.0
        estado = shipping_address.get("province", "") or ""
        envio_pendiente, paqueteria = obtener_tarifa_local(peso_total_kg, estado)
    else:
        logging.info(
            f"Pedido {order_id}: No se detectó 'preventa' en ningún shipping line. "
            "Costo de envío = 0."
        )

    total_pendiente = cantidad_pendiente_productos + envio_pendiente

    try:
        guardar_metafield_pedido_money(order_id, "cantidad_pendiente_productos", cantidad_pendiente_productos)
        guardar_metafield_pedido_money(order_id, "envio_pendiente", envio_pendiente)
        guardar_metafield_pedido_money(order_id, "pendiente_pago", total_pendiente)
        guardar_metafield_pedido_text(order_id, "paqueteria_", paqueteria)

        logging.info(
            f"Pedido {order_id}: Metafields (manual) configurados. Productos={cantidad_pendiente_productos}, "
            f"Envío={envio_pendiente}, Total={total_pendiente}, Paqueteria='{paqueteria}'"
        )
    except requests.exceptions.HTTPError as http_err:
        logging.error(
            f"Pedido {order_id}: Error HTTP al guardar metafields - {http_err} "
            f"Response Body: {http_err.response.text}"
        )
        return jsonify({"error": str(http_err), "response_body": http_err.response.text}), 500
    except Exception as e:
        logging.error(f"Pedido {order_id}: Error al guardar metafields manualmente - {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "status": "Metafields actualizados (manual)",
        "order_id": order_id,
        "cantidad_pendiente_productos": cantidad_pendiente_productos,
        "envio_pendiente": envio_pendiente,
        "pendiente_pago": total_pendiente,
        "paqueteria": paqueteria
    }), 200

###############################################################################
# 4. EJECUCIÓN DE LA APLICACIÓN FLASK
###############################################################################
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=True)
