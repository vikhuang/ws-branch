# flow_lab — 分點流量研究區(L3)

種子 = 2026-09-14~15 在 ws-quant 處置線孵出的全市場分點研究(原始 frontier
記帳與 findings 在 ws-quant `experiments/dispo_broker_overnight/`,此處為
研究複本;u3 的對帳結論已固化進 `measurement/identity.py` 與 ws-core 測試):

- `u1_retail_presence.py` + `findings/u1_retail_presence.md` — 處置四階段
  散戶化剖面(紅隊定稿:典型事件散戶化成立、金額口徑但書、baseline 污染教訓)
- `u1b_runup_gap.py` — 外資 HQ 處置期腰斬/解禁即回(作業性退場)
- `u2_retailization_arm.py` — 散戶化→右臂 null(家族 8/8;強制關卡示範)
- `u3_cohort_reconciliation.py` — cohort×官方三大法人對帳(0.97/空殼/0.58)

注意:u 系列腳本的 import 仍指向 ws-quant 路徑,在本 repo 不可直接執行——
P6 開張時改寫成吃 T1-T4 表的版本。家法(預期先行/獨立紅隊/trial ledger/
體檢先於使用)隨 REDESIGN §4 移植,新假說開跑前逐條照辦。
