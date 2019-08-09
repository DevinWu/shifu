/*
 * Copyright [2012-2014] PayPal Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.shifu.udf;

import ml.shifu.shifu.container.obj.ColumnConfig;
import ml.shifu.shifu.container.obj.ModelNormalizeConf.NormType;
import ml.shifu.shifu.core.DataPurifier;
import ml.shifu.shifu.core.DataSampler;
import ml.shifu.shifu.core.Normalizer;
import ml.shifu.shifu.exception.ShifuErrorCode;
import ml.shifu.shifu.exception.ShifuException;
import ml.shifu.shifu.udf.norm.CategoryMissingNormType;
import ml.shifu.shifu.udf.norm.PrecisionType;
import ml.shifu.shifu.udf.norm.WarnInNormalizeUDF;
import ml.shifu.shifu.util.CommonUtils;
import ml.shifu.shifu.util.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * NormalizeUDF class normalize the training data for parquet format.
 */
public class NormalizeUDF extends AbstractTrainerUDF<Tuple> {

    private static final String POSRATE = "posrate";
    private static final int MAX_MISMATCH_CNT = 500;
    public static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.######");

    private List<Set<String>> tags;

    private Double cutoff;
    private NormType normType;
    private PrecisionType precisionType;
    private int weightColumnId = -1;
    private List<DataPurifier> dataPurifiers;

    private boolean hasSegExpression = false;
    private boolean isCompactNorm = false;
    private boolean isLinearTarget = false;
    private int mismatchCnt = 0;

    private Map<String, List<String>> normVarNamesMapping;

    /**
     * For categorical feature, a map is used to save query time in execution
     */
    private Map<Integer, Map<String, Integer>> categoricalIndexMap = new HashMap<Integer, Map<String, Integer>>();

    /**
     * if current norm for only clean and not transform categorical and numeric value
     */
    private boolean isForClean = false;

    /**
     * In ZSCORE norm type, how to process category default missing value norm,
     * by default use mean, another option is POSRATE.
     */
    private CategoryMissingNormType categoryMissingNormType = CategoryMissingNormType.POSRATE;

    /**
     * Output compact column list for #isCompactNorm, schema is: tag, meta columns, feature list, weight
     */
    private List<String> outputCompactColumns;

    /**
     * Like schema in {@link #outputCompactColumns}, here is size of first tag and meta columns for output schema
     */
    private int cntOfTargetAndMetaColumns;

    public NormalizeUDF(String source, String pathModelConfig, String pathColumnConfig) throws Exception {
        this(source, pathModelConfig, pathColumnConfig, "false");
    }

    public NormalizeUDF(String source, String pathModelConfig, String pathColumnConfig, String isForClean)
            throws Exception {
        super(source, pathModelConfig, pathColumnConfig);

        this.categoryMissingNormType = CategoryMissingNormType
                .of(getUdfProperty(Constants.SHIFU_NORM_CATEGORY_MISSING_NORM, POSRATE));
        log.info("'categoryMissingNormType' is set to: " + this.categoryMissingNormType);

        this.precisionType = PrecisionType.of( // get precision type
                getUdfProperty(Constants.SHIFU_NORM_PRECISION_TYPE, PrecisionType.FLOAT32.toString()));
        log.info("Precision type is set to: " + this.precisionType);

        this.isForClean = "true".equalsIgnoreCase(isForClean);
        this.normType = modelConfig.getNormalizeType();
        log.debug("\t normType: " + normType.name());

        this.cutoff = modelConfig.getNormalizeStdDevCutOff();

        this.tags = super.modelConfig.getSetTags();

        boolean hasColumnSelected = false;
        for(ColumnConfig config: columnConfigList) {
            if(!hasColumnSelected && config.isFinalSelect()) {
                hasColumnSelected = true;
            }

            if(CommonUtils.isWeightColumn(modelConfig.getWeightColumnName(), config)) {
                this.weightColumnId = config.getColumnNum();
            }

            if(config.isCategorical()) {
                Map<String, Integer> map = new HashMap<String, Integer>();
                if(config.getBinCategory() != null) {
                    for(int i = 0; i < config.getBinCategory().size(); i++) {
                        List<String> catValues = CommonUtils.flattenCatValGrp(config.getBinCategory().get(i));
                        for(String cval: catValues) {
                            map.put(cval, i);
                        }
                    }
                }
                this.categoricalIndexMap.put(config.getColumnNum(), map);
            }
        }

        String filterExpressions = getUdfProperty(Constants.SHIFU_SEGMENT_EXPRESSIONS);
        if(StringUtils.isNotBlank(filterExpressions)) {
            this.hasSegExpression = true;
            String[] splits = CommonUtils.split(filterExpressions, Constants.SHIFU_STATS_FILTER_EXPRESSIONS_DELIMETER);
            this.dataPurifiers = new ArrayList<DataPurifier>(splits.length);
            for(String split: splits) {
                this.dataPurifiers.add(new DataPurifier(modelConfig, this.columnConfigList, split, false));
            }
        }

        this.isCompactNorm = Boolean.TRUE.toString() // is it "true/TRUE"?
                .equalsIgnoreCase(getUdfProperty(Constants.SHIFU_NORM_ONLY_SELECTED));
        // check if has final-select column, then enable real compact norm if user set,
        // isCompact now only works in non-tree model norm output
        this.isCompactNorm = (hasColumnSelected && this.isCompactNorm);

        // store schema list with format: <tag, meta columns, selected feature list, weight>
        if(this.isCompactNorm) {
            this.normVarNamesMapping = new HashMap<>();
            this.outputCompactColumns = new ArrayList<String>();
            this.outputCompactColumns.add( // add Target
                    CommonUtils.normColumnName(CommonUtils.findTargetColumn(columnConfigList).getColumnName()));
            for(ColumnConfig config: columnConfigList) {
                if(config.isMeta() && !config.isTarget()) { // add metas
                    this.outputCompactColumns.add(CommonUtils.normColumnName(config.getColumnName()));
                }
            }
            // set cnt for output schema reference
            this.cntOfTargetAndMetaColumns = this.outputCompactColumns.size();
            for(ColumnConfig config: columnConfigList) {
                if(config.isFinalSelect() && !config.isTarget() && !config.isMeta()) {
                    List<String> normVarNames = genNormColumnNames(config, normType);
                    this.outputCompactColumns.addAll(normVarNames);
                    this.normVarNamesMapping.put(config.getColumnName(), normVarNames);
                }
            }
            // add weight column as last
            this.outputCompactColumns.add("weight");
        }

        this.isLinearTarget = CommonUtils.isLinearTarget(modelConfig, columnConfigList);
    }

    @SuppressWarnings("deprecation")
    public Tuple exec(Tuple input) throws IOException {
        if(input == null || input.size() == 0) {
            return null;
        }

        Object tag = input.get(tagColumnNum);
        if(tag == null) {
            log.warn("The tag is NULL, just skip it!!");
            if(isPigEnabled(Constants.SHIFU_GROUP_COUNTER, "INVALID_TAG")) {
                PigStatusReporter.getInstance().getCounter(Constants.SHIFU_GROUP_COUNTER, "INVALID_TAG").increment(1);
            }
            return null;
        }
        final String rawTag = CommonUtils.trimTag(tag.toString());

        // make sure all invalid tag record are filter out
        if(!isLinearTarget && !super.tagSet.contains(rawTag)) {
            if(isPigEnabled(Constants.SHIFU_GROUP_COUNTER, "INVALID_TAG")) {
                PigStatusReporter.getInstance().getCounter(Constants.SHIFU_GROUP_COUNTER, "INVALID_TAG").increment(1);
            }
            return null;
        }

        // data sampling only for normalization, for data cleaning, shouldn't do data sampling
        // if(!isLinearTarget && !this.isForClean) { // do sampling for TREE model also - by huzza
        if(!isLinearTarget) {
            // do data sampling. Unselected data or data with invalid tag will be filtered out.
            boolean isNotSampled = DataSampler.isNotSampled(modelConfig.isRegression(), //
                    super.tagSet, super.posTagSet, super.negTagSet, // tags
                    modelConfig.getNormalizeSampleRate(), modelConfig.isNormalizeSampleNegOnly(), rawTag);
            if(isNotSampled) {
                return null;
            }
        }

        // append tuple with tag, normalized value.
        Tuple tuple = TupleFactory.getInstance().newTuple();
        String weightRaw = null;
        Map<String, Object> compactVarMap = null;
        if(this.isCompactNorm) {
            compactVarMap = new HashMap<String, Object>();
        }

        int inputSize = input.size();
        // no segment expressions, the data size should exactly same as len(columnConfigList)
        if(!this.hasSegExpression) {
            if(inputSize != this.columnConfigList.size()) {
                log.error("the input size - " + input.size() + ", while column size - " + columnConfigList.size());
                this.mismatchCnt++;
                // Throw exceptions if the mismatch count is greater than MAX_MISMATCH_CNT,
                // this could make Shifu could skip some malformed data
                if(this.mismatchCnt > MAX_MISMATCH_CNT) {
                    throw new ShifuException(ShifuErrorCode.ERROR_NO_EQUAL_COLCONFIG);
                }
                return null;
            }
        }

        for(int i = 0; i < this.columnConfigList.size(); i++) {
            int dataIndex = ((this.hasSegExpression) ? i % inputSize : i);
            String val = (input.get(dataIndex) == null) ? "" : input.get(dataIndex).toString();
            ColumnConfig config = columnConfigList.get(i);

            if(this.weightColumnId > 0 && this.weightColumnId == config.getColumnNum()) {
                // has weight column and this is weight column, hold the value
                weightRaw = val;
            }

            if(config.isTarget()) { // target column
                if(modelConfig.isRegression()) { // regression model
                    int type = 0;
                    if(super.posTagSet.contains(rawTag)) {
                        type = 1;
                    } else if(super.negTagSet.contains(rawTag)) {
                        type = 0;
                    } else {
                        log.error("Invalid data! The target value is not listed - " + rawTag);
                        warn("Invalid data! The target value is not listed - " + rawTag,
                                WarnInNormalizeUDF.INVALID_TAG);
                        return null;
                    }
                    if(this.isCompactNorm) {
                        compactVarMap.put(CommonUtils.normColumnName(config.getColumnName()), type);
                    } else {
                        tuple.append(type);
                    }
                } else if(this.isLinearTarget) { // linear model
                    double tagValue = 0.0;
                    try {
                        tagValue = Double.parseDouble(rawTag);
                    } catch (Exception e) {
                        log.error("Tag - " + rawTag + " is invalid(not numerical). Skip record.");
                        // skip this line
                        return null;
                    }
                    if(this.isCompactNorm) {
                        compactVarMap.put(CommonUtils.normColumnName(config.getColumnName()), tagValue);
                    } else {
                        tuple.append(tagValue);
                    }
                } else { // classification model
                    int index = -1;
                    for(int j = 0; j < tags.size(); j++) {
                        Set<String> tagSet = tags.get(j);
                        if(tagSet.contains(rawTag)) {
                            index = j;
                            break;
                        }
                    }
                    if(index == -1) {
                        log.error("Invalid data! The target value is not listed - " + rawTag);
                        warn("Invalid data! The target value is not listed - " + rawTag,
                                WarnInNormalizeUDF.INVALID_TAG);
                        return null;
                    }
                    if(this.isCompactNorm) {
                        compactVarMap.put(CommonUtils.normColumnName(config.getColumnName()), index);
                    } else {
                        tuple.append(index);
                    }
                }
                continue;
            }

            if(this.isForClean) { // for RF/GBT model, only clean data, not real do norm data
                if(config.isCategorical()) {
                    Map<String, Integer> map = this.categoricalIndexMap.get(config.getColumnNum());
                    // map should not be null, no need check if map is null, if val not in binCategory, set it to ""
                    tuple.append(((map.get(val) == null || map.get(val) == -1)) ? "" : val);
                } else {
                    Double normVal = 0d;
                    try {
                        normVal = Double.parseDouble(val);
                    } catch (Exception e) {
                        log.debug("Not decimal format " + val + ", using default!");
                        normVal = Normalizer.defaultMissingValue(config);
                    }

                    appendOutputValue(tuple, normVal, true);
                }
            } else { // for NN/LR model, needs to do data normalization
                if(this.isCompactNorm) { // compact format <target, meta, select_vars>
                    // only output features and target, weight in compact norm mode
                    if(!config.isMeta() && config.isFinalSelect()) {
                        // for multiple classification, binPosRate means rate of such category over all counts,
                        // reuse binPosRate for normalize
                        List<Double> normVals = Normalizer.fullNormalize(config, val, cutoff, normType,
                                this.categoryMissingNormType, this.categoricalIndexMap.get(config.getColumnNum()));
                        List<String> formatNormVals = new ArrayList<>();
                        for(Double normVal: normVals) {
                            String formatVal = getOutputValue(normVal, true);
                            formatNormVals.add(formatVal);
                        }

                        List<String> normVarNames = this.normVarNamesMapping.get(config.getColumnName());
                        assert formatNormVals.size() != normVarNames.size();
                        for(int k = 0; k < normVarNames.size(); k++) {
                            compactVarMap.put(normVarNames.get(k), formatNormVals.get(k));
                        }
                    } else if(config.isMeta()) {
                        compactVarMap.put(CommonUtils.normColumnName(config.getColumnName()), val);
                    } else { // skip others
                        // if is compact mode but such column is not final selected, should be empty, as only append
                        // target, meta and finalSelect feature, no need append here so this code block is empty.
                    }
                } else {
                    // append normalize data. exclude data clean,
                    // for data cleaning, no need check good or bad candidate
                    // By zhanhu: fix bug - if the ForceSelect variables exists in candidates list,
                    // it will cause variable fail to normalize
                    if(CommonUtils.isToNormVariable(config, super.hasCandidates, modelConfig.isRegression())) {
                        // for multiple classification, binPosRate means rate of such category over all counts,
                        // reuse binPosRate for normalize
                        List<Double> normVals = Normalizer.fullNormalize(config, val, cutoff, normType,
                                this.categoryMissingNormType, this.categoricalIndexMap.get(config.getColumnNum()));
                        for(Double normVal: normVals) {
                            appendOutputValue(tuple, normVal, true);
                        }
                    } else {
                        tuple.append(config.isMeta() ? val : null);
                    }
                }
            }
        }

        // for compact norm mode, output to tuple at here
        if(this.isCompactNorm) {
            for(int i = 0; i < this.outputCompactColumns.size(); i++) {
                tuple.append(compactVarMap.get(this.outputCompactColumns.get(i)));
            }
        }

        // append tuple with weight.
        double weight = 1.0d;
        if(this.weightColumnId > 0) {
            try {
                weight = Double.parseDouble(weightRaw);
            } catch (Exception e) {
                if(System.currentTimeMillis() % 100 == 0) { // avoid error log flood
                    log.error("Incorrect weight column - " + weightRaw, e);
                }
                weight = 1.0d; // set to 1.0d as default
            }
        }
        tuple.append(weight);

        return tuple;
    }

    /**
     * FLOAT7 is old with DecimalFormat, new one with FLOAT16, FLOAT32, DOUBLE64
     */
    private String getOutputValue(double value, boolean enablePrecision) {
        if(enablePrecision) {
            switch(this.getPrecisionType()) {
                case FLOAT7:
                    return DECIMAL_FORMAT.format(value);
                case FLOAT16:
                    return "" + toFloat(fromFloat((float) value));
                case DOUBLE64:
                    return value + "";
                case FLOAT32:
                default:
                    return ((float) value) + "";
            }
        } else {
            return ((float) value) + "";
        }
    }

    /**
     * FLOAT7 is old with DecimalFormat, new one with FLOAT16, FLOAT32, DOUBLE64
     */
    private void appendOutputValue(Tuple tuple, double value, boolean enablePrecision) {
        if(enablePrecision) {
            switch(this.getPrecisionType()) {
                case FLOAT7:
                    tuple.append(DECIMAL_FORMAT.format(value));
                    break;
                case FLOAT16:
                    tuple.append(toFloat(fromFloat((float) value)));
                    break;
                case DOUBLE64:
                    tuple.append(value);
                    break;
                case FLOAT32:
                default:
                    tuple.append((float) value);
                    break;
            }
        } else {
            tuple.append((float) value);
        }
    }

    // returns all higher 16 bits as 0 for all results
    public static int fromFloat(float fval) {
        int fbits = Float.floatToIntBits(fval);
        int sign = fbits >>> 16 & 0x8000; // sign only
        int val = (fbits & 0x7fffffff) + 0x1000; // rounded value

        if(val >= 0x47800000) // might be or become NaN/Inf
        { // avoid Inf due to rounding
            if((fbits & 0x7fffffff) >= 0x47800000) { // is or must become NaN/Inf
                if(val < 0x7f800000) // was value but too large
                    return sign | 0x7c00; // make it +/-Inf
                return sign | 0x7c00 | // remains +/-Inf or NaN
                        (fbits & 0x007fffff) >>> 13; // keep NaN (and Inf) bits
            }
            return sign | 0x7bff; // unrounded not quite Inf
        }
        if(val >= 0x38800000) // remains normalized value
            return sign | val - 0x38000000 >>> 13; // exp - 127 + 15
        if(val < 0x33000000) // too small for subnormal
            return sign; // becomes +/-0
        val = (fbits & 0x7fffffff) >>> 23; // tmp exp for subnormal calc
        return sign | ((fbits & 0x7fffff | 0x800000) // add subnormal bit
                + (0x800000 >>> val - 102) // round depending on cut off
        >>> 126 - val); // div by 2^(1-(exp-127+15)) and >> 13 | exp=0
    }

    // ignores the higher 16 bits
    public static float toFloat(int hbits) {
        int mant = hbits & 0x03ff; // 10 bits mantissa
        int exp = hbits & 0x7c00; // 5 bits exponent
        if(exp == 0x7c00) {// NaN/Inf
            exp = 0x3fc00; // -> NaN/Inf
        } else if(exp != 0) { // normalized value
            exp += 0x1c000; // exp - 15 + 127
            if(mant == 0 && exp > 0x1c400) // smooth transition
                return Float.intBitsToFloat((hbits & 0x8000) << 16 | exp << 13 | 0x3ff);
        } else if(mant != 0) { // && exp==0 -> subnormal
            exp = 0x1c400; // make it normal
            do {
                mant <<= 1; // mantissa * 2
                exp -= 0x400; // decrease exp by 1
            } while((mant & 0x400) == 0); // while not normal
            mant &= 0x3ff; // discard subnormal bit
        } // else +/-0 -> +/-0
        return Float.intBitsToFloat( // combine all parts
                (hbits & 0x8000) << 16 // sign << ( 31 - 15 )
                        | (exp | mant) << 13); // value << ( 23 - 10 )
    }

    public Schema outputSchema(Schema input) {
        try {
            StringBuilder schemaStr = new StringBuilder();
            Schema tupleSchema = null;
            if(this.isCompactNorm) {
                // compact norm, no need to Normalized schema
                tupleSchema = new Schema();
            } else {
                schemaStr.append("Normalized:Tuple(");
            }

            if(this.isCompactNorm) {
                // compact norm mode, schema is tag, meta columns, feature columns and weight
                for(int i = 0; i < outputCompactColumns.size(); i++) {
                    String normName = CommonUtils.normColumnName(outputCompactColumns.get(i));
                    if(i == 0) {
                        // target column
                        tupleSchema.add(new Schema.FieldSchema(normName, DataType.DOUBLE));
                    } else if(i < cntOfTargetAndMetaColumns) {
                        // meta column
                        tupleSchema.add(new Schema.FieldSchema(normName, DataType.CHARARRAY));
                    } else {
                        // feature column
                        switch(this.getPrecisionType()) {
                            case DOUBLE64:
                                tupleSchema.add(new Schema.FieldSchema(normName, DataType.DOUBLE));
                                break;
                            case FLOAT7:
                            case FLOAT16:
                            case FLOAT32:
                            default:
                                // all these types are actually float in Java/Pig
                                tupleSchema.add(new Schema.FieldSchema(normName, DataType.FLOAT));
                        }
                    }
                }
                Schema schema = new Schema(new Schema.FieldSchema("Normalized", tupleSchema, DataType.TUPLE));
                return schema;
            } else {
                for(ColumnConfig config: columnConfigList) {
                    if(config.isMeta()) {
                        schemaStr.append(CommonUtils.normColumnName(config.getColumnName()) + ":chararray" + ",");
                    } else if(config.isTarget()) {
                        schemaStr.append(CommonUtils.normColumnName(config.getColumnName()) + ":double" + ",");
                    } else if(this.isForClean) { // for tree model, doesn't support ONEHOT
                        String normalName = CommonUtils.normColumnName(config.getColumnName());
                        if(config.isCategorical()) {
                            schemaStr.append(normalName + ":chararray" + ",");
                        } else {
                            schemaStr.append(normalName + ":" + getOutputPrecisionType() + ",");
                        }
                    } else {
                        if(CommonUtils.isToNormVariable(config, super.hasCandidates, modelConfig.isRegression())) {
                            List<String> normColumnNames = this.genNormColumnNames(config, this.normType);
                            for(String normalName: normColumnNames) {
                                schemaStr.append(normalName + ":" + getOutputPrecisionType() + ",");
                            }
                        } else {
                            schemaStr.append(CommonUtils.normColumnName(config.getColumnName()) + ":chararray" + ",");
                        }
                    }
                }
                schemaStr.append("shifu::weight:").append(getOutputPrecisionType()).append(")");
                // log.info(" schema string is " + schemaStr.toString());
                return Utils.getSchemaFromString(schemaStr.toString());
            }
        } catch (Exception e) {
            log.error("error in outputSchema", e);
            return null;
        }
    }

    private String getOutputPrecisionType() {
        switch(this.getPrecisionType()) {
            case DOUBLE64:
                return "double";
            case FLOAT7:
            case FLOAT16:
            case FLOAT32:
                // all these types are actually
            default:
                return "float";
        }
    }

    public PrecisionType getPrecisionType() {
        return precisionType;
    }

    public void setPrecisionType(PrecisionType precisionType) {
        this.precisionType = precisionType;
    }
}
