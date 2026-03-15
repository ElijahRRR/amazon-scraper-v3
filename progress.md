# Project Progress

## Current Status
- Total: 22 features
- Passing: 0 / 22 (0%)
- Current: Starting feature #1

## v2 Analysis Summary

### Key Problems Identified
1. **分页性能**: batch筛选通过EXISTS子查询关联tasks表，每行result都要扫描tasks表
2. **变动筛选慢**: change字段无独立索引，OR条件多列查询无法走索引
3. **OFFSET分页**: 大数据量下OFFSET越大越慢
4. **解析不稳**: 单一解析路径，非标准页面fallback不足
5. **截图不可靠**: 文件系统IPC无状态追踪，失败静默丢失
6. **无batch_asins关联表**: batch和result的关系通过tasks间接查询

### v3 Architecture Design
- **数据库**: asin_data(主表) + asin_history(历史) + asin_changes(预计算变动) + batches + batch_asins(直接关联) + tasks + screenshots
- **分页**: keyset pagination (WHERE id > last_id) 替代 OFFSET
- **筛选**: batch筛选通过batch_asins JOIN，变动筛选通过asin_changes表
- **解析**: 多策略cascade: JSON-LD → CSS选择器 → 正则 → meta/hidden data
- **截图**: screenshots表追踪状态，失败可重试，有明确状态机

## Session Log
### Session 1 - 2026-03-15
- Completed: v2 analysis, feature list creation
- Next: feature #1 (项目结构和配置)
