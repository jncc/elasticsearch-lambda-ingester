package search.ingester.models.validators;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.*;

@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = NotBlankIfAnotherFieldIsBlankValidator.class)
@Documented
public @interface NotBlankIfAnotherFieldIsBlank {
    String fieldName();
    String dependFieldName();

    String message() default "{NotBlankIfAnotherFieldIsBlank.message}";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    @Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @interface List {
        NotBlankIfAnotherFieldIsBlank[] value();
    }

}