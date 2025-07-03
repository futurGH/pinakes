export function toDateOrNull(ts: string | number | undefined | null): Date | null {
	const date = new Date(ts ?? NaN);
	return isNaN(date.getTime()) ? null : date;
}
