"""
Streamlit Dashboard - Thống kê bất động sản.
Chạy: streamlit run dashboard.py  (cổng 8501)
"""
import pathlib
import sys
import urllib.parse
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from pyarrow import fs



st.set_page_config(page_title="Real Estate Analytics", layout="wide")



import tempfile
import os
import requests

# Load data bằng pandas tối ưu qua WebHDFS (Xử lý lỗi chặn NameNode/DataNode trên máy cục bộ)
@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame | None:
    try:
        base_url = "http://localhost:9870/webhdfs/v1/data/real-estate"
        
        # Hàm đệ quy tải thư mục Parquet từ WebHDFS và tự động fix hostname
        def download_hdfs(url, local_path):
            res = requests.get(f"{url}?op=LISTSTATUS").json()
            if "FileStatuses" not in res: return
            for f in res["FileStatuses"]["FileStatus"]:
                # Bỏ qua _SUCCESS
                if f["pathSuffix"] == "_SUCCESS": continue
                
                # Cần encode URL cho thư mục phân mảnh có chứa tiếng Việt
                item_url = f"{url}/{urllib.parse.quote(f['pathSuffix'])}"
                
                if f["type"] == "DIRECTORY":
                    new_local = os.path.join(local_path, f["pathSuffix"])
                    os.makedirs(new_local, exist_ok=True)
                    download_hdfs(item_url, new_local)
                elif f["type"] == "FILE" and f["pathSuffix"].endswith(".parquet"):
                    # Chặn tự động redirect để sửa `datanode` thành `localhost`
                    r1 = requests.get(f"{item_url}?op=OPEN", allow_redirects=False)
                    if r1.status_code == 307:
                        redirect_url = r1.headers["Location"].replace("http://datanode:", "http://localhost:")
                        r2 = requests.get(redirect_url)
                        if r2.status_code == 200:
                            with open(os.path.join(local_path, f["pathSuffix"]), "wb") as out:
                                out.write(r2.content)

        # Tạo thư mục tạm để tải Parquet
        temp_dir = tempfile.mkdtemp(prefix="real_estate_")
        download_hdfs(base_url, temp_dir)

        # Đọc dữ liệu từ file Parquet tải về
        required_cols = [
            "list_id", "title", "property_type", "district", 
            "price", "area_m2", "rooms", "url", "listing_type"
        ]
        
        # Pandas tự động gom data từ các thư mục phân mảnh (property_type=...)
        raw_df = pd.read_parquet(temp_dir, engine="pyarrow", columns=required_cols)
        
        if raw_df.empty:
            return None

        df = raw_df.copy()
        
        if "property_type" in df.columns:
            df["property_type"] = df["property_type"].astype(str)
        
        df["property_type"] = df["property_type"].replace(["nan", "None", "<NA>", ""], "Khác").fillna("Khác")
        df["district"] = df["district"].astype(str).fillna("").replace(["nan", "None", "<NA>"], "")

        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
        df["area_m2"] = pd.to_numeric(df["area_m2"], errors="coerce").fillna(0)
        df["price_ty"] = df["price"] / 1e9
        df["price_per_m2_trieu"] = (df["price"] / df["area_m2"]).ffill().fillna(0) / 1e6
        
        return df
        
    except Exception as e:
        st.error(f"⚠️ Lỗi xử lý dữ liệu: {e}")
        return None


# Main
def main():
    st.title("🏠 Real Estate Big Data Analytics")
    st.caption(f"Cập nhật: {datetime.now():%Y-%m-%d %H:%M:%S}")

    with st.spinner("Đang tải dữ liệu từ Hadoop HDFS..."):
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
    
    # Định dạng hiển thị metric theo hình thức Bán/Thuê
    if ltype == "Cho thuê":
        c3.metric("Giá thuê TB", f"{pdf['price'].mean()/1e6:.1f} tr/tháng")
    else:
        c3.metric("Giá bán TB", f"{pdf['price_ty'].mean():.2f} tỷ")
        
    if pdf["area_m2"].notna().any():
        c4.metric("Diện tích TB", f"{pdf['area_m2'].mean():.1f} m²")

    st.markdown("---")

    # ---- Tabs ----
    t1, t2, t3, t4 = st.tabs(
        ["📍 Theo khu vực", "🏢 Theo loại hình", "📊 Phan bổ giá & Diện tích", "📋 Bảng chi tiết"]
    )

    with t1:
        # Thiết lập TOP_N mặc định là 10
        top_n = 10
        
        by_dist = (
            pdf[pdf["district"] != ""]
            .groupby("district")
            .agg(n=("list_id", "count"), avg_price=("price_ty", "mean"))
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
        st.plotly_chart(
            px.bar(by_dist.sort_values("avg_price"), x="avg_price", y="district",
                   orientation="h",
                   title="Giá trung bình theo khu vực (tỷ VND)",
                   labels={"avg_price": "Giá TB (tỷ)", "district": "Khu vực"},
                   color="avg_price", color_continuous_scale="Reds"),
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
        priced = pdf[pdf["price_ty"].notna() & (pdf["price_ty"] > 0)]
        if not priced.empty:
            cap    = priced["price_ty"].quantile(0.95)
            clip   = priced[priced["price_ty"] <= cap]
            st.plotly_chart(
                px.histogram(clip, x="price_ty", nbins=40,
                             title="Biểu đồ phân bổ giá (Bỏ 5% nhóm giá quá cao để tránh lệch biểu đồ)",
                             labels={"price_ty": "Giá (tỷ VND)"},
                             color_discrete_sequence=["#1f77b4"]),
                use_container_width=True,
            )
            
        scatter = pdf[pdf["area_m2"].notna() & (pdf["area_m2"] > 0)].copy()
        if not scatter.empty:
            area_cap = scatter["area_m2"].quantile(0.95)
            scatter  = scatter[scatter["area_m2"] <= area_cap]
            st.plotly_chart(
                px.scatter(scatter, x="area_m2", y="price_ty", color="property_type",
                           title="Tương quan giữa Diện tích và Giá bán",
                           labels={"area_m2": "Diện tích (m²)", "price_ty": "Giá (tỷ VND)"},
                           opacity=0.6),
                use_container_width=True,
            )

    with t4:
        show_cols = ["title", "property_type", "district",
                     "price_ty", "area_m2", "rooms", "url"]
        show_cols = [c for c in show_cols if c in pdf.columns]
        
        # Cấu hình hiển thị link url đẹp hơn trong bảng dữ liệu của Streamlit
        st.dataframe(
            pdf[show_cols].rename(columns={
                "title":         "Tiêu đề",
                "property_type": "Loại hình",
                "district":      "Khu vực",
                "price_ty":      "Giá (tỷ)",
                "area_m2":       "DT (m²)",
                "rooms":         "Phòng",
                "url":           "Đường dẫn gốc",
            }),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()