import { RecurrenceExpressionTypeId } from "$lib/api/enums";

export class RecurrenceExpressionUtil {
	static formatExpression(typeId?: string, expression?: string): string {
		if (!typeId) return expression ?? "Unknown";

		if (typeId === RecurrenceExpressionTypeId.NaturalCron) return expression ?? "—";
		if (typeId === RecurrenceExpressionTypeId.TimeSpanInterval) return expression ?? "—";
		if (typeId === RecurrenceExpressionTypeId.NeverRecurs) return "Never";

		return `${expression ?? "—"} (${typeId})`;
	}
}