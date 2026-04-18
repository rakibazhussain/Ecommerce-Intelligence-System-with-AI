from flask import Flask, jsonify
from processing.data_processor import load_data
from ai_engine.decision_engine import analyze_data
from database.db import save_data, save_insights  
from kafka_module.producer import send_event   # ✅ ADD THIS

app = Flask(__name__)

@app.route('/favicon.ico')
def favicon():
    return '', 204


@app.route("/")
def run_pipeline():
    try:
        data = load_data("data/dataset.csv")

        save_data(data)

        results = analyze_data(data)

        save_insights(results)

        # 🔥 SEND TO KAFKA (IMPORTANT)
        for r in results:
            message = f"{r['type']} → {r['product_name']} (Sales: {r['sales']}) [{r['section']}]"

            if r['type'] == "Trending":
                send_event("trending", message)

            elif r['type'] == "Low Sales":
                send_event("low_sales", message)

        return jsonify({
            "status": "Pipeline executed successfully ✅",
            "total_records": len(data),
            "insights_generated": len(results)
        })

    except Exception as e:
        return jsonify({
            "status": "Error ❌",
            "message": str(e)
        })


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)