export interface AlertRowView {
	rowId: string;
	user: string;
	profileAddress: string;
	addrShort: string;
	volumeFormatted: string;
	outcomeClass: string;
	outcomeLabel: string;
	statusBadgeHtml: string;
	dateText: string;
	timeText: string;
	detailHtml: string;
	expanded: boolean;
	question: string;
	timestamp: number;
	conditionId: string;
	priceFormatted: string;
	volume: number;
	price: number;
}
