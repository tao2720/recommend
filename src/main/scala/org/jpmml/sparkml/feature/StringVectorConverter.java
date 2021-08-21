package org.jpmml.sparkml.feature;

import org.apache.spark.ml.feature.StringVector;
import org.jpmml.sparkml.FeatureConverter;
import org.jpmml.converter.Feature;
import org.jpmml.sparkml.FeatureConverter;
import org.jpmml.sparkml.SparkMLEncoder;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description: JPMML 字符串转向量的Convert
 * @Author: QF
 * @Date: 2020/8/3 8:58 AM
 * @Version V1.0
 */
public class StringVectorConverter extends FeatureConverter<StringVector> {

    public StringVectorConverter(StringVector transformer){
        super(transformer);
    }

    @Override
    public List<Feature> encodeFeatures(SparkMLEncoder encoder){
        StringVector transformer = getTransformer();

        List<Feature> result = new ArrayList<Feature>();
        String[] inputCols = transformer.getInputCols();
        for(String inputCol : inputCols){
            List<Feature> features = encoder.getFeatures(inputCol);
            result.addAll(features);
        }

        return result;
    }
}
