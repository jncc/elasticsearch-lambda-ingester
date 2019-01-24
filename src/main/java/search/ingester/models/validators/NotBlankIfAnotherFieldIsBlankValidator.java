package search.ingester.models.validators;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.lang.reflect.InvocationTargetException;

public class NotBlankIfAnotherFieldIsBlankValidator implements ConstraintValidator<NotBlankIfAnotherFieldIsBlank, Object> {
    private String fieldName;
    private String dependFieldName;

    @Override
    public void initialize(NotBlankIfAnotherFieldIsBlank annotation) {
        fieldName          = annotation.fieldName();
        dependFieldName    = annotation.dependFieldName();
    }


    @Override
    public boolean isValid(Object value, ConstraintValidatorContext ctx) {

        try {
            String fieldValue       = BeanUtils.getProperty(value, fieldName);
            String dependFieldValue = BeanUtils.getProperty(value, dependFieldName);

            if (StringUtils.isBlank(fieldValue) && StringUtils.isBlank(dependFieldValue)) {
                ctx.disableDefaultConstraintViolation();
                ctx.buildConstraintViolationWithTemplate(ctx.getDefaultConstraintMessageTemplate())
                        .addNode(dependFieldName)
                        .addConstraintViolation();
                return false;
            }

        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }

        return true;
    }
}
