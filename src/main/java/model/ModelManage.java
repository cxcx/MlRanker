package model;

import org.apache.logging.log4j.Logger;
import org.dmg.pmml.FieldName;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.Loggers;
import org.jpmml.evaluator.*;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class ModelManage {
    private static Logger LOGGER = Loggers.getLogger(ModelManage.class, "Ranker");

    private static Evaluator evaluator = null;

    public static void loadPmmlFile(String modelName) {
        if (evaluator != null ) {
            return;
        }
        Path path = PathUtils.get(new File(ModelManage.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent());
        String url = null;
        try {
            url = path.resolve(modelName).toFile().getCanonicalPath();
            LOGGER.error("model file url : [" + url + "]");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (url == null) {
            LOGGER.error("Cant get model file : [" + modelName + "]");
            return;
        }

        File pmmlFile = new File(url);
        try {
            evaluator = new LoadingModelEvaluatorBuilder().load(pmmlFile).build();
            evaluator.verify();
        } catch (IOException | JAXBException | SAXException e) {
            e.printStackTrace();
        }

    }


    public static double inference(List<Double> inputs) {
        Object val = 0.0;

        List<? extends InputField> inputFields = evaluator.getInputFields();
        if (inputFields.size() != inputs.size()) {
            throw  new IllegalArgumentException(String.format("model evaluator has wrong inputs, model need %d input field but got %d fields",
                    inputFields.size(), inputs.size()));
        }

        Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();
        for (int i = 0; i < inputFields.size(); i++) {
            arguments.put(inputFields.get(i).getFieldName(), inputFields.get(i).prepare(inputs.get(i)));
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        List<? extends TargetField> targetFields = evaluator.getTargetFields();
        if (targetFields.size() != 1) {
            throw  new IllegalArgumentException(String.format("model evaluator has wrong outputs, got %d output field", targetFields.size()));
        }

        val = results.get(targetFields.get(0).getName());

        return (Double)val;
    }

    public static void main(String[] args) {
        loadPmmlFile("lr.pmml");
        List<Double> testInputs = new ArrayList<>();
        testInputs.add(0.1);
        testInputs.add(0.1);
        System.out.println(inference(testInputs));
    }
}
