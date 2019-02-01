/*
 * Copyright [2013-2019] PayPal Software Foundation
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
package ml.shifu.shifu.core.dtrain.wnd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

import ml.shifu.guagua.ComputableMonitor;
import ml.shifu.guagua.hadoop.io.GuaguaLineRecordReader;
import ml.shifu.guagua.hadoop.io.GuaguaWritableAdapter;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.util.MemoryLimitedList;
import ml.shifu.guagua.util.NumberFormatUtils;
import ml.shifu.guagua.worker.AbstractWorkerComputable;
import ml.shifu.guagua.worker.WorkerContext;
import ml.shifu.shifu.container.obj.ColumnConfig;
import ml.shifu.shifu.container.obj.ModelConfig;
import ml.shifu.shifu.container.obj.RawSourceData.SourceType;
import ml.shifu.shifu.core.dtrain.CommonConstants;
import ml.shifu.shifu.core.dtrain.DTrainUtils;
import ml.shifu.shifu.core.dtrain.nn.NNConstants;
import ml.shifu.shifu.util.CommonUtils;
import ml.shifu.shifu.util.Constants;
import ml.shifu.shifu.util.MapReduceUtils;

/**
 * {@link WNDWorker} is responsible for loading part of data into memory, do iteration gradients computation and send
 * back to master for master aggregation. After master aggregation is done, received latest weights to do next
 * iteration.
 * 
 * <p>
 * {@link WNDWorker} needs to be recovered like load snapshot models and load data into memory again and can be used to
 * train with current iterations. All fault tolerance related state recovery should be taken care in this worker.
 * 
 * <p>
 * Data loading into memory as memory list includes two parts: numerical float array and sparse input object array which
 * is for categorical variables. To leverage sparse feature of categorical variables, sparse object is leveraged to
 * save memory and matrix computation.
 * 
 * <p>
 * TODO mini batch matrix support, matrix computation support
 * TODO forward, backward abstraction
 * TODO embedding arch logic
 * TODO variable/field based optimization to compute gradients
 * 
 * @author Zhang David (pengzhang@paypal.com)
 */
@ComputableMonitor(timeUnit = TimeUnit.SECONDS, duration = 3600)
public class WNDWorker extends
        AbstractWorkerComputable<WNDParams, WNDParams, GuaguaWritableAdapter<LongWritable>, GuaguaWritableAdapter<Text>> {

    protected static final Logger LOG = LoggerFactory.getLogger(WNDWorker.class);

    /**
     * Model configuration loaded from configuration file.
     */
    private ModelConfig modelConfig;

    /**
     * Column configuration loaded from configuration file.
     */
    private List<ColumnConfig> columnConfigList;

    /**
     * Basic input count for final-select variables or good candidates(if no any variables are selected)
     */
    protected int inputCount;

    /**
     * Basic numerical input count for final-select variables or good candidates(if no any variables are selected)
     */
    protected int numInputs;

    /**
     * Basic categorical input count
     */
    protected int categoricalInputCount;

    /**
     * Means if do variable selection, if done, many variables will be set to finalSelect = true; if not, no variables
     * are selected and should be set to all good candidate variables.
     */
    private boolean isAfterVarSelect = true;

    /**
     * input record size, inc one by one.
     */
    protected long count;

    /**
     * sampled input record size.
     */
    protected long sampleCount;

    /**
     * Positive count in training data list, only be effective in 0-1 regression or onevsall classification
     */
    protected long positiveTrainCount;

    /**
     * Positive count in training data list and being selected in training, only be effective in 0-1 regression or
     * onevsall classification
     */
    protected long positiveSelectedTrainCount;

    /**
     * Negative count in training data list , only be effective in 0-1 regression or onevsall classification
     */
    protected long negativeTrainCount;

    /**
     * Negative count in training data list and being selected, only be effective in 0-1 regression or onevsall
     * classification
     */
    protected long negativeSelectedTrainCount;

    /**
     * Positive count in validation data list, only be effective in 0-1 regression or onevsall classification
     */
    protected long positiveValidationCount;

    /**
     * Negative count in validation data list, only be effective in 0-1 regression or onevsall classification
     */
    protected long negativeValidationCount;

    /**
     * Training data set with only in memory, in the future, MemoryDiskList can be leveraged.
     */
    private volatile MemoryLimitedList<Data> trainingData;

    /**
     * Validation data set with only in memory.
     */
    private volatile MemoryLimitedList<Data> validationData;

    /**
     * Mapping for (ColumnNum, Map(Category, CategoryIndex) for categorical feature
     */
    private Map<Integer, Map<String, Integer>> columnCategoryIndexMapping;

    /**
     * A splitter to split data with specified delimiter.
     */
    private Splitter splitter;

    /**
     * Index map in which column index and data input array index for fast location.
     */
    private ConcurrentMap<Integer, Integer> inputIndexMap = new ConcurrentHashMap<Integer, Integer>();

    /**
     * Trainer id used to tag bagging training job, starting from 0, 1, 2 ...
     */
    private int trainerId = 0;

    /**
     * If has candidate in column list.
     */
    private boolean hasCandidates;

    /**
     * Indicates if validation are set by users for validationDataPath, not random picking
     */
    protected boolean isManualValidation = false;

    /**
     * If stratified sampling or random sampling
     */
    private boolean isStratifiedSampling = false;

    /**
     * If k-fold cross validation
     */
    private boolean isKFoldCV;

    /**
     * Construct a validation random map for different classes. For stratified sampling, this is useful for each class
     * sampling.
     */
    private Map<Integer, Random> validationRandomMap = new HashMap<Integer, Random>();

    /**
     * Random object to sample negative records
     */
    protected Random negOnlyRnd = new Random(System.currentTimeMillis() + 1000L);

    /**
     * Whether to enable poisson bagging with replacement.
     */
    protected boolean poissonSampler;

    /**
     * PoissonDistribution which is used for poisson sampling for bagging with replacement.
     */
    protected PoissonDistribution rng = null;

    /**
     * PoissonDistribution which is used for up sampling positive records.
     */
    protected PoissonDistribution upSampleRng = null;

    /**
     * Parameters defined in ModelConfig.json#train part
     */
    private Map<String, Object> validParams;

    /**
     * WideAndDeep graph definition network.
     */
    private WideAndDeep wnd;

    /**
     * Logic to load data into memory list which includes float array for numerical features and sparse object array for
     * categorical features.
     */
    @Override
    public void load(GuaguaWritableAdapter<LongWritable> currentKey, GuaguaWritableAdapter<Text> currentValue,
            WorkerContext<WNDParams, WNDParams> context) {
        if((++this.count) % 5000 == 0) {
            LOG.info("Read {} records.", this.count);
        }

        // hashcode for fixed input split in train and validation
        long hashcode = 0;
        float[] inputs = new float[this.numInputs];
        SparseInput[] cateInputs = new SparseInput[this.categoricalInputCount];
        float ideal = 0f, significance = 1f;
        int index = 0, numericalIndex = 0, cateIndex = 0;
        // use guava Splitter to iterate only once
        for(String input: this.splitter.split(currentValue.getWritable().toString())) {
            if(index == this.columnConfigList.size()) {
                significance = getWeightValue(input);
                break; // the last field is significance, break here
            } else {
                ColumnConfig config = this.columnConfigList.get(index);
                if(config != null && config.isTarget()) {
                    ideal = getFloatValue(input);
                } else {
                    // final select some variables but meta and target are not included
                    if(validColumn(config)) {
                        if(config.isNumerical()) {
                            inputs[numericalIndex] = getFloatValue(input);
                            this.inputIndexMap.putIfAbsent(config.getColumnNum(), numericalIndex++);
                        } else if(config.isCategorical()) {
                            cateInputs[cateIndex] = new SparseInput(config.getColumnNum(), getCateIndex(input, config));
                            this.inputIndexMap.putIfAbsent(config.getColumnNum(), cateIndex++);
                        }
                        hashcode = hashcode * 31 + input.hashCode();
                    }
                }
            }
            index += 1;
        }

        // output delimiter in norm can be set by user now and if user set a special one later changed, this exception
        // is helped to quick find such issue.
        validateInputLength(context, inputs, numericalIndex);

        // sample negative only logic here
        if(sampleNegOnly(hashcode, ideal)) {
            return;
        }
        // up sampling logic, just add more weights while bagging sampling rate is still not changed
        if(modelConfig.isRegression() && isUpSampleEnabled() && Double.compare(ideal, 1d) == 0) {
            // ideal == 1 means positive tags; sample + 1 to avoid sample count to 0
            significance = significance * (this.upSampleRng.sample() + 1);
        }

        Data data = new Data(inputs, cateInputs, significance, ideal);
        // split into validation and training data set according to validation rate
        boolean isInTraining = this.addDataPairToDataSet(hashcode, data, context.getAttachment());
        // update some positive or negative selected count in metrics
        this.updateMetrics(data, isInTraining);
    }

    protected boolean isUpSampleEnabled() {
        // only enabled in regression
        return this.upSampleRng != null && (modelConfig.isRegression()
                || (modelConfig.isClassification() && modelConfig.getTrain().isOneVsAll()));
    }

    private boolean sampleNegOnly(long hashcode, float ideal) {
        boolean ret = false;
        if(modelConfig.getTrain().getSampleNegOnly()) {
            double bagSampleRate = this.modelConfig.getBaggingSampleRate();
            if(this.modelConfig.isFixInitialInput()) {
                // if fixInitialInput, sample hashcode in 1-sampleRate range out if negative records
                int startHashCode = (100 / this.modelConfig.getBaggingNum()) * this.trainerId;
                // here BaggingSampleRate means how many data will be used in training and validation, if it is 0.8, we
                // should take 1-0.8 to check endHashCode
                int endHashCode = startHashCode + Double.valueOf((1d - bagSampleRate) * 100).intValue();
                if((modelConfig.isRegression()
                        || (modelConfig.isClassification() && modelConfig.getTrain().isOneVsAll()))
                        && (int) (ideal + 0.01d) == 0 && isInRange(hashcode, startHashCode, endHashCode)) {
                    ret = true;
                }
            } else {
                // if not fixed initial input, for regression or onevsall multiple classification, if negative record
                if((modelConfig.isRegression()
                        || (modelConfig.isClassification() && modelConfig.getTrain().isOneVsAll()))
                        && (int) (ideal + 0.01d) == 0 && negOnlyRnd.nextDouble() > bagSampleRate) {
                    ret = true;
                }
            }
        }
        return ret;
    }

    private void updateMetrics(Data data, boolean isInTraining) {
        // do bagging sampling only for training data
        if(isInTraining) {
            // for training data, compute real selected training data according to baggingSampleRate
            if(isPositive(data.label)) {
                this.positiveSelectedTrainCount += 1L;
            } else {
                this.negativeSelectedTrainCount += 1L;
            }
        } else {
            // for validation data, according bagging sampling logic, we may need to sampling validation data set, while
            // validation data set are only used to compute validation error, not to do real sampling is ok.
        }
    }

    /**
     * Add to training set or validation set according to validation rate.
     * 
     * @param hashcode
     *            the hash code of the data
     * @param data
     *            data instance
     * @param isValidation
     *            if it is validation
     * @return if in training, training is true, others are false.
     */
    protected boolean addDataPairToDataSet(long hashcode, Data data, Object attachment) {
        // if validation data from configured validation data set
        boolean isValidation = (attachment != null && attachment instanceof Boolean) ? (Boolean) attachment : false;

        if(this.isKFoldCV) {
            int k = this.modelConfig.getTrain().getNumKFold();
            if(hashcode % k == this.trainerId) {
                this.validationData.append(data);
                if(isPositive(data.label)) {
                    this.positiveValidationCount += 1L;
                } else {
                    this.negativeValidationCount += 1L;
                }
                return false;
            } else {
                this.trainingData.append(data);
                if(isPositive(data.label)) {
                    this.positiveTrainCount += 1L;
                } else {
                    this.negativeTrainCount += 1L;
                }
                return true;
            }
        }

        if(this.isManualValidation) {
            if(isValidation) {
                this.validationData.append(data);
                if(isPositive(data.label)) {
                    this.positiveValidationCount += 1L;
                } else {
                    this.negativeValidationCount += 1L;
                }
                return false;
            } else {
                this.trainingData.append(data);
                if(isPositive(data.label)) {
                    this.positiveTrainCount += 1L;
                } else {
                    this.negativeTrainCount += 1L;
                }
                return true;
            }
        } else {
            if(Double.compare(this.modelConfig.getValidSetRate(), 0d) != 0) {
                int classValue = (int) (data.label + 0.01f);
                Random random = null;
                if(this.isStratifiedSampling) {
                    // each class use one random instance
                    random = validationRandomMap.get(classValue);
                    if(random == null) {
                        random = new Random();
                        this.validationRandomMap.put(classValue, random);
                    }
                } else {
                    // all data use one random instance
                    random = validationRandomMap.get(0);
                    if(random == null) {
                        random = new Random();
                        this.validationRandomMap.put(0, random);
                    }
                }

                if(this.modelConfig.isFixInitialInput()) {
                    // for fix initial input, if hashcode%100 is in [start-hashcode, end-hashcode), validation,
                    // otherwise training. start hashcode in different job is different to make sure bagging jobs have
                    // different data. if end-hashcode is over 100, then check if hashcode is in [start-hashcode, 100]
                    // or [0, end-hashcode]
                    int startHashCode = (100 / this.modelConfig.getBaggingNum()) * this.trainerId;
                    int endHashCode = startHashCode
                            + Double.valueOf(this.modelConfig.getValidSetRate() * 100).intValue();
                    if(isInRange(hashcode, startHashCode, endHashCode)) {
                        this.validationData.append(data);
                        if(isPositive(data.label)) {
                            this.positiveValidationCount += 1L;
                        } else {
                            this.negativeValidationCount += 1L;
                        }
                        return false;
                    } else {
                        this.trainingData.append(data);
                        if(isPositive(data.label)) {
                            this.positiveTrainCount += 1L;
                        } else {
                            this.negativeTrainCount += 1L;
                        }
                        return true;
                    }
                } else {
                    // not fixed initial input, if random value >= validRate, training, otherwise validation.
                    if(random.nextDouble() >= this.modelConfig.getValidSetRate()) {
                        this.trainingData.append(data);
                        if(isPositive(data.label)) {
                            this.positiveTrainCount += 1L;
                        } else {
                            this.negativeTrainCount += 1L;
                        }
                        return true;
                    } else {
                        this.validationData.append(data);
                        if(isPositive(data.label)) {
                            this.positiveValidationCount += 1L;
                        } else {
                            this.negativeValidationCount += 1L;
                        }
                        return false;
                    }
                }
            } else {
                this.trainingData.append(data);
                if(isPositive(data.label)) {
                    this.positiveTrainCount += 1L;
                } else {
                    this.negativeTrainCount += 1L;
                }
                return true;
            }
        }
    }

    private boolean isPositive(float value) {
        return Float.compare(1f, value) == 0 ? true : false;
    }

    private boolean isInRange(long hashcode, int startHashCode, int endHashCode) {
        // check if in [start, end] or if in [start, 100) and [0, end-100)
        int hashCodeIn100 = (int) hashcode % 100;
        if(endHashCode <= 100) {
            // in range [start, end)
            return hashCodeIn100 >= startHashCode && hashCodeIn100 < endHashCode;
        } else {
            // in range [start, 100) or [0, endHashCode-100)
            return hashCodeIn100 >= startHashCode || hashCodeIn100 < (endHashCode % 100);
        }
    }

    /**
     * If no enough columns for model training, most of the cases root cause is from inconsistent delimiter.
     */
    private void validateInputLength(WorkerContext<WNDParams, WNDParams> context, float[] inputs, int numInputIndex) {
        if(numInputIndex != inputs.length) {
            String delimiter = context.getProps().getProperty(Constants.SHIFU_OUTPUT_DATA_DELIMITER,
                    Constants.DEFAULT_DELIMITER);
            throw new RuntimeException("Input length is inconsistent with parsing size. Input original size: "
                    + inputs.length + ", parsing size:" + numInputIndex + ", delimiter:" + delimiter + ".");
        }
    }

    /**
     * If column is valid and be selected in model training
     */
    private boolean validColumn(ColumnConfig columnConfig) {
        if(isAfterVarSelect) {
            return columnConfig != null && !columnConfig.isMeta() && !columnConfig.isTarget()
                    && columnConfig.isFinalSelect();
        } else {
            return !columnConfig.isMeta() && !columnConfig.isTarget()
                    && CommonUtils.isGoodCandidate(columnConfig, this.hasCandidates);
        }
    }

    private int getCateIndex(String input, ColumnConfig columnConfig) {
        int shortValue = (columnConfig.getBinCategory().size());
        if(input.length() == 0) {
            // empty string
            shortValue = columnConfig.getBinCategory().size();
        } else {
            Integer categoricalIndex = this.columnCategoryIndexMapping.get(columnConfig.getColumnNum()).get(input);
            if(categoricalIndex == null) {
                shortValue = -1; // invalid category, set to -1 for last index
            } else {
                shortValue = categoricalIndex.intValue();
            }
            if(shortValue == -1) {
                // not found
                shortValue = columnConfig.getBinCategory().size();
            }
        }
        return shortValue;
    }

    private float getWeightValue(String input) {
        float significance = 1f;
        if(StringUtils.isNotBlank(modelConfig.getWeightColumnName())) {
            // check here to avoid bad performance in failed NumberFormatUtils.getFloat(input, 1f)
            significance = input.length() == 0 ? 1f : NumberFormatUtils.getFloat(input, 1f);
            // if invalid weight, set it to 1f and warning in log
            if(Float.compare(significance, 0f) < 0) {
                LOG.warn("Record {} in current worker weight {} is less than 0 and invalid, set it to 1.", count,
                        significance);
                significance = 1f;
            }
        }
        return significance;
    }

    private float getFloatValue(String input) {
        // check here to avoid bad performance in failed NumberFormatUtils.getFloat(input, 0f)
        float floatValue = input.length() == 0 ? 0f : NumberFormatUtils.getFloat(input, 0f);
        // no idea about why NaN in input data, we should process it as missing value TODO , according to norm type
        return (Float.isNaN(floatValue) || Double.isNaN(floatValue)) ? 0f : floatValue;
    }

    @Override
    public void initRecordReader(GuaguaFileSplit fileSplit) throws IOException {
        // initialize Hadoop based line (long, string) reader
        super.setRecordReader(new GuaguaLineRecordReader(fileSplit));
    }

    /*
     * (non-Javadoc)
     * 
     * @see ml.shifu.guagua.worker.AbstractWorkerComputable#init(ml.shifu.guagua.worker.WorkerContext)
     */
    @SuppressWarnings({ "unchecked", "unused" })
    @Override
    public void init(WorkerContext<WNDParams, WNDParams> context) {
        Properties props = context.getProps();
        try {
            SourceType sourceType = SourceType
                    .valueOf(props.getProperty(CommonConstants.MODELSET_SOURCE_TYPE, SourceType.HDFS.toString()));
            this.modelConfig = CommonUtils.loadModelConfig(props.getProperty(CommonConstants.SHIFU_MODEL_CONFIG),
                    sourceType);
            this.columnConfigList = CommonUtils
                    .loadColumnConfigList(props.getProperty(CommonConstants.SHIFU_COLUMN_CONFIG), sourceType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.initCateIndexMap();
        this.hasCandidates = CommonUtils.hasCandidateColumns(columnConfigList);

        // create Splitter
        String delimiter = context.getProps().getProperty(Constants.SHIFU_OUTPUT_DATA_DELIMITER);
        this.splitter = MapReduceUtils.generateShifuOutputSplitter(delimiter);

        Integer kCrossValidation = this.modelConfig.getTrain().getNumKFold();
        if(kCrossValidation != null && kCrossValidation > 0) {
            isKFoldCV = true;
            LOG.info("Cross validation is enabled by kCrossValidation: {}.", kCrossValidation);
        }

        this.poissonSampler = Boolean.TRUE.toString()
                .equalsIgnoreCase(context.getProps().getProperty(NNConstants.NN_POISON_SAMPLER));
        this.rng = new PoissonDistribution(1.0d);
        Double upSampleWeight = modelConfig.getTrain().getUpSampleWeight();
        if(Double.compare(upSampleWeight, 1d) != 0 && (modelConfig.isRegression()
                || (modelConfig.isClassification() && modelConfig.getTrain().isOneVsAll()))) {
            // set mean to upSampleWeight -1 and get sample + 1to make sure no zero sample value
            LOG.info("Enable up sampling with weight {}.", upSampleWeight);
            this.upSampleRng = new PoissonDistribution(upSampleWeight - 1);
        }

        this.trainerId = Integer.valueOf(context.getProps().getProperty(CommonConstants.SHIFU_TRAINER_ID, "0"));

        double memoryFraction = Double.valueOf(context.getProps().getProperty("guagua.data.memoryFraction", "0.6"));
        LOG.info("Max heap memory: {}, fraction: {}", Runtime.getRuntime().maxMemory(), memoryFraction);

        double validationRate = this.modelConfig.getValidSetRate();
        if(StringUtils.isNotBlank(modelConfig.getValidationDataSetRawPath())) {
            // fixed 0.6 and 0.4 of max memory for trainingData and validationData
            this.trainingData = new MemoryLimitedList<Data>(
                    (long) (Runtime.getRuntime().maxMemory() * memoryFraction * 0.6), new ArrayList<Data>());
            this.validationData = new MemoryLimitedList<Data>(
                    (long) (Runtime.getRuntime().maxMemory() * memoryFraction * 0.4), new ArrayList<Data>());
        } else {
            if(Double.compare(validationRate, 0d) != 0) {
                this.trainingData = new MemoryLimitedList<Data>(
                        (long) (Runtime.getRuntime().maxMemory() * memoryFraction * (1 - validationRate)),
                        new ArrayList<Data>());
                this.validationData = new MemoryLimitedList<Data>(
                        (long) (Runtime.getRuntime().maxMemory() * memoryFraction * validationRate),
                        new ArrayList<Data>());
            } else {
                this.trainingData = new MemoryLimitedList<Data>(
                        (long) (Runtime.getRuntime().maxMemory() * memoryFraction), new ArrayList<Data>());
            }
        }

        int[] inputOutputIndex = DTrainUtils.getNumericAndCategoricalInputAndOutputCounts(this.columnConfigList);
        // numerical + categorical = # of all input
        this.numInputs = inputOutputIndex[0];
        this.inputCount = inputOutputIndex[0] + inputOutputIndex[1];
        // regression outputNodeCount is 1, binaryClassfication, it is 1, OneVsAll it is 1, Native classification it is
        // 1, with index of 0,1,2,3 denotes different classes
        this.isAfterVarSelect = (inputOutputIndex[3] == 1);
        this.isManualValidation = (modelConfig.getValidationDataSetRawPath() != null
                && !"".equals(modelConfig.getValidationDataSetRawPath()));

        this.isStratifiedSampling = this.modelConfig.getTrain().getStratifiedSample();

        this.validParams = this.modelConfig.getTrain().getParams();

        // Build wide and deep graph
        List<Integer> embedColumnIds = (List<Integer>) this.validParams.get(CommonConstants.NUM_EMBED_COLUMN_IDS);
        Integer embedOutputs = (Integer) this.validParams.get(CommonConstants.NUM_EMBED_OUTPUTS);
        List<Integer> embedOutputList = new ArrayList<Integer>();
        for(Integer cId: embedColumnIds) {
            embedOutputList.add(embedOutputs == null ? 16 : embedOutputs);
        }
        List<Integer> wideColumnIds = DTrainUtils.getCategoricalIds(columnConfigList, isAfterVarSelect);
        int numLayers = (Integer) this.validParams.get(CommonConstants.NUM_HIDDEN_LAYERS);
        List<String> actFunc = (List<String>) this.validParams.get(CommonConstants.ACTIVATION_FUNC);
        List<Integer> hiddenNodes = (List<Integer>) this.validParams.get(CommonConstants.NUM_HIDDEN_NODES);
        Float l2reg = (Float) this.validParams.get(CommonConstants.NUM_HIDDEN_LAYERS);
        this.wnd = new WideAndDeep(columnConfigList, numInputs, embedColumnIds, embedOutputList, wideColumnIds,
                hiddenNodes, actFunc, l2reg);
    }

    private void initCateIndexMap() {
        this.columnCategoryIndexMapping = new HashMap<Integer, Map<String, Integer>>();
        for(ColumnConfig config: this.columnConfigList) {
            if(config.isCategorical() && config.getBinCategory() != null) {
                Map<String, Integer> tmpMap = new HashMap<String, Integer>();
                for(int i = 0; i < config.getBinCategory().size(); i++) {
                    List<String> catVals = CommonUtils.flattenCatValGrp(config.getBinCategory().get(i));
                    for(String cval: catVals) {
                        tmpMap.put(cval, i);
                    }
                }
                this.columnCategoryIndexMapping.put(config.getColumnNum(), tmpMap);
            }
        }
    }

    @Override
    public WNDParams doCompute(WorkerContext<WNDParams, WNDParams> context) {
        if(context.isFirstIteration()) {
            return new WNDParams();
        }

        // update master global model into worker WideAndDeep graph
        this.wnd.updateWeights(context.getLastMasterResult());

        // compute gradients for each iteration
        WideAndDeep gradientsWnd = new WideAndDeep(); // TODO, construct a wideanddeep graph, how to aggregate gradients
                                                      // to params
        int trainCnt = trainingData.size(), validCnt = validationData.size();
        double trainSumError = 0d, validSumError = 0d;
        for(Data data: trainingData) {
            float[] logits = this.wnd.forward(data.getNumericalValues(), getEmbedInputs(data), getWideInputs(data));
            float error = sigmoid(logits[0]) - data.label;
            trainSumError += data.weight * error * error; // TODO, logloss, squredloss, weighted error or not
            this.wnd.backward(new float[] { error }, data.getWeight());
        }

        // compute validation error
        for(Data data: validationData) {
            float[] logits = this.wnd.forward(data.getNumericalValues(), getEmbedInputs(data), getWideInputs(data));
            float error = sigmoid(logits[0]) - data.label;
            validSumError += data.weight * error * error;
        }

        // set cnt, error to params and return to master
        WNDParams params = new WNDParams();
        params.setTrainCount(trainCnt);
        params.setValidationCount(validCnt);
        params.setTrainError(trainSumError);
        params.setValidationError(validSumError);
        params.setWnd(gradientsWnd);
        return params;
    }

    public float sigmoid(float logit) {
        return (float) (1 / (1 + Math.min(1.0E19, Math.exp(-logit))));
    }

    private List<SparseInput> getWideInputs(Data data) {
        List<SparseInput> wideInputs = new ArrayList<SparseInput>();
        for(Integer columnId: this.wnd.getWideColumnIds()) {
            wideInputs.add(data.getCategoricalValues()[this.inputIndexMap.get(columnId)]);
        }
        return wideInputs;
    }

    private List<SparseInput> getEmbedInputs(Data data) {
        List<SparseInput> embedInputs = new ArrayList<SparseInput>();
        for(Integer columnId: this.wnd.getEmbedColumnIds()) {
            embedInputs.add(data.getCategoricalValues()[this.inputIndexMap.get(columnId)]);
        }
        return embedInputs;
    }

    /**
     * TODO
     * 
     * @author Zhang David (pengzhang@paypal.com)
     */
    public static class Data {

        /**
         * Numerical values
         */
        private float[] numericalValues;

        /**
         * Categorical values in sparse object
         */
        private SparseInput[] categoricalValues;

        /**
         * The weight of one training record like dollar amount in one txn
         */
        private float weight;

        /**
         * Target value of one record
         */
        private float label;

        /**
         * Constructor for a unified data object which is for a line of training record.
         * 
         * @param numericalValues
         *            numerical values
         * @param categoricalValues
         *            categorical values which stored into one {@link SparseInput} array.
         * @param weight
         *            the weight of one training record
         */
        public Data(float[] numericalValues, SparseInput[] categoricalValues, float weight, float ideal) {
            this.numericalValues = numericalValues;
            this.categoricalValues = categoricalValues;
            this.weight = weight;
            this.label = ideal;
        }

        /**
         * @return the numericalValues
         */
        public float[] getNumericalValues() {
            return numericalValues;
        }

        /**
         * @param numericalValues
         *            the numericalValues to set
         */
        public void setNumericalValues(float[] numericalValues) {
            this.numericalValues = numericalValues;
        }

        /**
         * @return the categoricalValues
         */
        public SparseInput[] getCategoricalValues() {
            return categoricalValues;
        }

        /**
         * @param categoricalValues
         *            the categoricalValues to set
         */
        public void setCategoricalValues(SparseInput[] categoricalValues) {
            this.categoricalValues = categoricalValues;
        }

        /**
         * @return the weight
         */
        public float getWeight() {
            return weight;
        }

        /**
         * @param weight
         *            the weight to set
         */
        public void setWeight(float weight) {
            this.weight = weight;
        }

        /**
         * @return the ideal
         */
        public float getLabel() {
            return label;
        }

        /**
         * @param ideal
         *            the ideal to set
         */
        public void setLabel(float ideal) {
            this.label = ideal;
        }

    }

}