"""
Streamlit Dashboard - Thống kê bất động sản.
Chạy từ thư mục gốc: streamlit run dashboard/dashboard.py  (cổng 8501)
"""
import os
import pathlib
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from data_access import IncrementalWebHdfsCache, load_dataset

st.set_page_config(page_title="Real Estate Analytics", layout="wide")

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
DATA_SOURCE = os.getenv("DASHBOARD_DATA_SOURCE", "hdfs").strip().lower()
LOCAL_DATA_PATH = pathlib.Path(
    os.getenv("LOCAL_DATA_PATH", str(PROJECT_ROOT / "spark" / "output" / "real-estate"))
)
WEBHDFS_URL = os.getenv(
    "WEBHDFS_URL", "http://localhost:9870/webhdfs/v1/data/real-estate"
).rstrip("/")
CACHE_PATH = os.getenv("DASHBOARD_CACHE_PATH") or None
DATANODE_HOST = os.getenv("WEBHDFS_DATANODE_HOST", "localhost")


# Hỗ trợ hai chế độ thống nhất với Spark: HDFS (Docker, mặc định) hoặc local.
@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame | None:
    try:
        df = load_dataset(
            DATA_SOURCE,
            LOCAL_DATA_PATH,
            WEBHDFS_URL,
            CACHE_PATH,
            DATANODE_HOST,
        )
        return None if df.empty else df
        
    except Exception as e:
        st.error(f"⚠️ Lỗi xử lý dữ liệu: {e}")
        return None


# Main
def main():
    st.title("🏠 Real Estate Big Data Analytics")
    st.caption(f"Cập nhật: {datetime.now():%Y-%m-%d %H:%M:%S}")

    source_label = "Hadoop HDFS" if DATA_SOURCE == "hdfs" else "Parquet local"
    with st.spinner(f"Đang tải dữ liệu từ {source_label}..."):
        pdf_all = load_data()

    if pdf_all is None or pdf_all.empty:
        st.warning("Chưa có dữ liệu. Hãy đảm bảo Crawler -> Kafka -> Spark Consumer đã đẩy dữ liệu vào HDFS.")
        return

    # ---- Sidebar ----
    with st.sidebar:
        st.header("⚙️ Điều khiển")
        if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        if DATA_SOURCE == "hdfs" and st.button(
            "🧹 Xóa cache HDFS", use_container_width=True
        ):
            IncrementalWebHdfsCache(
                WEBHDFS_URL, CACHE_PATH, DATANODE_HOST
            ).clear()
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        ltype = st.radio("Bộ lọc hình thức:", ["Bán", "Cho thuê", "Tất cả"], index=0)

    # ---- Lọc ----
    if ltype != "Tất cả":
        pdf = pdf_all[pdf_all["listing_type"] == ltype].copy()
    else:
        pdf = pdf_all.copy()

    if pdf.empty:
        st.warning(f"Không có tin nào thuộc hình thức: {ltype}")
        return

    st.caption(f"Đang xem: **{ltype}** · Tổng số **{len(pdf):,}** bản ghi")

    # ---- Metrics ----
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng số tin",  f"{len(pdf):,}")
    c2.metric("Số khu vực (Quận/Huyện)",   f"{pdf['district'].nunique():,}")
    
    valid_area = pdf.loc[pdf["area_m2"] > 0, "area_m2"]
    if ltype == "Cho thuê":
        valid_price = pdf.loc[pdf["price"] > 0, "price_trieu"]
        c3.metric("Giá thuê TB", f"{valid_price.mean():.1f} tr/tháng" if not valid_price.empty else "N/A")
        c4.metric("Diện tích TB", f"{valid_area.mean():.1f} m²" if not valid_area.empty else "N/A")
    elif ltype == "Bán":
        valid_price = pdf.loc[pdf["price"] > 0, "price_ty"]
        c3.metric("Giá bán TB", f"{valid_price.mean():.2f} tỷ" if not valid_price.empty else "N/A")
        c4.metric("Diện tích TB", f"{valid_area.mean():.1f} m²" if not valid_area.empty else "N/A")
    else:
        sales = pdf.loc[(pdf["listing_type"] == "Bán") & (pdf["price"] > 0), "price_ty"]
        rentals = pdf.loc[(pdf["listing_type"] == "Cho thuê") & (pdf["price"] > 0), "price_trieu"]
        c3.metric("Giá bán TB", f"{sales.mean():.2f} tỷ" if not sales.empty else "N/A")
        c4.metric("Giá thuê TB", f"{rentals.mean():.1f} tr/tháng" if not rentals.empty else "N/A")

    st.markdown("---")

    # ---- Tabs ----
    t1, t2, t3, t4 = st.tabs(
        ["📍 Theo khu vực", "🏢 Theo loại hình", "📊 Phân bổ giá & Diện tích", "📋 Bảng chi tiết"]
    )

    with t1:
        # Thiết lập TOP_N mặc định là 10
        top_n = 10
        
        by_dist = (
            pdf[pdf["district"] != ""]
            .groupby("district")
            .agg(n=("list_id", "count"))
            .reset_index()
            .sort_values("n", ascending=False)
            .head(top_n)
        )
        st.plotly_chart(
            px.bar(by_dist, x="n", y="district", orientation="h",
                   title=f"Top {top_n} Khu vực có số lượng tin nhiều nhất",
                   labels={"n": "Số lượng tin", "district": "Khu vực"},
                   color="n", color_continuous_scale="Viridis"),
            use_container_width=True,
        )
        price_groups = []
        if ltype in {"Bán", "Tất cả"}:
            price_groups.append(("Bán", "price_ty", "tỷ VND"))
        if ltype in {"Cho thuê", "Tất cả"}:
            price_groups.append(("Cho thuê", "price_trieu", "triệu/tháng"))
        for group_type, price_col, unit in price_groups:
            group_data = pdf[
                (pdf["listing_type"] == group_type)
                & (pdf["district"] != "")
                & (pdf["price"] > 0)
            ]
            avg_by_dist = (
                group_data.groupby("district")[price_col]
                .mean().reset_index(name="avg_price")
                .sort_values("avg_price")
                .tail(top_n)
            )
            if not avg_by_dist.empty:
                st.plotly_chart(
                    px.bar(
                        avg_by_dist, x="avg_price", y="district", orientation="h",
                        title=f"Giá {group_type.lower()} trung bình theo khu vực ({unit})",
                        labels={"avg_price": f"Giá TB ({unit})", "district": "Khu vực"},
                        color="avg_price", color_continuous_scale="Reds",
                    ),
                    use_container_width=True,
                )

    with t2:
        by_type = (
            pdf.groupby("property_type")
            .size().reset_index(name="n")
            .sort_values("n", ascending=False)
        )
        st.plotly_chart(
            px.pie(by_type, values="n", names="property_type",
                   title="Tỷ trọng tin đăng theo loại hình bất động sản",
                   hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel),
            use_container_width=True,
        )

    with t3:
        analysis_type = "Bán" if ltype == "Tất cả" else ltype
        analysis_df = pdf[pdf["listing_type"] == analysis_type].copy()
        price_col = "price_ty" if analysis_type == "Bán" else "price_trieu"
        price_unit = "tỷ VND" if analysis_type == "Bán" else "triệu/tháng"
        priced = analysis_df[analysis_df["price"] > 0]
        if not priced.empty:
            cap = priced[price_col].quantile(0.95)
            clip = priced[priced[price_col] <= cap]
            st.plotly_chart(
                px.histogram(clip, x=price_col, nbins=40,
                             title=f"Phân bổ giá {analysis_type.lower()} (loại 5% ngoại lệ cao)",
                             labels={price_col: f"Giá ({price_unit})"},
                             color_discrete_sequence=["#1f77b4"]),
                use_container_width=True,
            )
            
        scatter = analysis_df[
            (analysis_df["area_m2"] > 0) & (analysis_df["price"] > 0)
        ].copy()
        if not scatter.empty:
            area_cap = scatter["area_m2"].quantile(0.95)
            scatter  = scatter[scatter["area_m2"] <= area_cap]
            st.plotly_chart(
                px.scatter(scatter, x="area_m2", y=price_col, color="property_type",
                           title=f"Tương quan giữa diện tích và giá {analysis_type.lower()}",
                           labels={"area_m2": "Diện tích (m²)", price_col: f"Giá ({price_unit})"},
                           opacity=0.6),
                use_container_width=True,
            )

    with t4:
        show_price_col = "price_trieu" if ltype == "Cho thuê" else "price_ty"
        show_cols = ["title", "property_type", "district",
                     show_price_col, "area_m2", "rooms", "url"]
        show_cols = [c for c in show_cols if c in pdf.columns]
        
        # Cấu hình hiển thị link url đẹp hơn trong bảng dữ liệu của Streamlit
        st.dataframe(
            pdf[show_cols].rename(columns={
                "title":         "Tiêu đề",
                "property_type": "Loại hình",
                "district":      "Khu vực",
                "price_ty":      "Giá (tỷ)",
                "price_trieu":   "Giá (triệu/tháng)",
                "area_m2":       "DT (m²)",
                "rooms":         "Phòng",
                "url":           "Đường dẫn gốc",
            }),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
