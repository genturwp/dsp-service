from eagle.extensions import db
from sqlalchemy.sql import func


class RawDsp(db.Model):
    __tablename__ = "raw_dsp"
    __table_args__ = {"schema": "raw"}

    id = db.Column(db.Integer, primary_key=True)
    dsp_nomor = db.Column(db.String(50), nullable=True)
    dsp_jabatan = db.Column(db.String(255), nullable=True, index=True)
    dsp_gol_jab = db.Column(db.String(255), nullable=True)
    dsp_pangkat = db.Column(db.String(255), nullable=True)
    dsp_korps = db.Column(db.String(255), nullable=True)
    dsp_bidang_profesi = db.Column(db.String(255), nullable=True)
    dsp_spesialisasi = db.Column(db.String(255), nullable=True)
    dsp_pa = db.Column(db.Double, nullable=True)
    dsp_ba = db.Column(db.Double, nullable=True)
    dsp_ta = db.Column(db.Double, nullable=True)
    dsp_pns = db.Column(db.Double, nullable=True)
    dsp_jml = db.Column(db.Double, nullable=True)
    dsp_ket = db.Column(db.Text, nullable=True)
    dsp_satuankerja_id = db.Column(db.String(255), nullable=True, index=True)
    dsp_satuankerja_nama = db.Column(db.String(300), nullable=True)
    dsp_nomor_keputusan_kasau = db.Column(db.String(300), nullable=True, index=True)
    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now())
    updated_at = db.Column(db.DateTime(timezone=True), server_default=func.now())
    deleted_at = db.Column(db.DateTime(timezone=True), nullable=True)
    file_name = db.Column(db.String(500), nullable=True)
    dsp_subsatuankerja_nama = db.Column(db.String(500), nullable=True)
    dsp_subsatuankerja_id = db.Column(db.String(255), nullable=True)

    def __init__(
        self,
        id,
        dsp_nomor,
        dsp_jabatan,
        dsp_gol_jab,
        dsp_pangkat,
        dsp_korps,
        dsp_bidang_profesi,
        dsp_spesialisasi,
        dsp_pa,
        dsp_ba,
        dsp_ta,
        dsp_pns,
        dsp_jml,
        dsp_ket,
        dsp_satuankerja_id,
        dsp_satuankerja_nama,
        dsp_nomor_keputusan_kasau,
        created_at,
        updated_at,
        deleted_at,
        file_name,
        dsp_subsatuankerja_nama,
        dsp_subsatuankerja_id,
    ):
        self.id = id
        self.dsp_nomor = dsp_nomor
        self.dsp_jabatan = dsp_jabatan
        self.dsp_gol_jab = dsp_gol_jab
        self.dsp_pangkat = dsp_pangkat
        self.dsp_korps = dsp_korps
        self.dsp_bidang_profesi = dsp_bidang_profesi
        self.dsp_spesialisasi = dsp_spesialisasi
        self.dsp_pa = dsp_pa
        self.dsp_ba = dsp_ba
        self.dsp_ta = dsp_ta
        self.dsp_pns = dsp_pns
        self.dsp_jml = dsp_jml
        self.dsp_ket = dsp_ket
        self.dsp_satuankerja_id = dsp_satuankerja_id
        self.dsp_satuankerja_nama = dsp_satuankerja_nama
        self.dsp_nomor_keputusan_kasau = dsp_nomor_keputusan_kasau
        self.created_at = created_at
        self.updated_at = updated_at
        self.deleted_at = deleted_at
        self.file_name = file_name
        self.dsp_subsatuankerja_nama = dsp_subsatuankerja_nama
        self.dsp_subsatuankerja_id = dsp_subsatuankerja_id

    def __repr__(self):
        return f'<RawDsp "{self.id}">'
